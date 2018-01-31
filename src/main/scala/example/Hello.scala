package example

import java.util.UUID

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.util.{Success, Failure}
import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import example.Exercise2.PetOwnerLoader

object Hello extends App {
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val system = ActorSystem("testSystem")

  try {
    val _petOwnerLoader = PetOwnerLoader.start(4)
    val f = Future.sequence((1 to 5).map(i => Exercise2.getPetOwner(i.toString))).map(_.flatten)
    val petOwners = Await.result(f, 10 seconds)
    println(s"petOwners: \n\t${petOwners.mkString("\n\t")}")
    println("Done.")
  } finally {
    system.terminate()
  }
}

object Exercise2 {
  import scala.concurrent.ExecutionContext.Implicits.global

  case class User(id: String, name: String, petIds: List[String])
  case class Pet(id: String, name: String)
  case class PetOwner(user: User, pets: List[Pet])

  def getUser(id: String): Future[Option[User]] = {
    Future {
      val petIds = (1 to 6).map(i => s"pet_${id}_$i").toList
      Some(User(id, s"username_$id", petIds))
    }
  }

  def getPet(id: String): Future[Option[Pet]] = {
    Future {
      Some(Pet(id, s"petname_$id"))
    }
  }

  // wrap apis above in actors
  // implement PetOwnerLoader to load petowners so that at most 4 pets are loaded concurrently
  // simpler version: 4 pets loaded concurrently per request
  // (n.b.: this can be done very easily using streams, you can show how to do that as well,
  // but the point of this exercise is implementing an actor)
  // how would you change it so that at most 4 pets are loaded concurrently for all requests
  def getPetOwner(userId: String)
                 (implicit system: ActorSystem): Future[Option[PetOwner]] = {
    getUser(userId).flatMap({
      case None => Future { None }
      case Some(user) => PetOwnerLoader.loadPets(user).map(Some(_))
    })
  }

//  Public Interface
  object PetOwnerLoader {
    object Messages {
      case class LoadPets(user: User)
      case class PetOwnerResponse(petOwner: PetOwner)
    }

    implicit val timeout = Timeout(5 seconds)

    def start(maxConcurrentRequests: Int)
             (implicit system: ActorSystem): ActorRef = {
      require(maxConcurrentRequests > 0)
      system.actorOf(PetOwnerLoader.props(maxConcurrentRequests), "petOwnerLoader")
    }

    def loadPets(user: User)
                (implicit system: ActorSystem): Future[PetOwner] = {
      ask(system.actorSelection(petOwnerLoaderPath), Messages.LoadPets(user)).map {
        case Messages.PetOwnerResponse(petOwner) => petOwner
      }
    }

    private def props(maxConcurrentRequests: Int): Props =
      Props(new PetOwnerLoader(maxConcurrentRequests))

    private val petOwnerLoaderPath = "/user/petOwnerLoader"
  }

//  Actor implementation
  class PetOwnerLoader(val maxConcurrentRequests: Int) extends Actor with ActorLogging {
    import PetOwnerLoader.Messages._

    case class TaskInProgress(rId: String,
                              sender: ActorRef,
                              user: User,
                              fetchedPets: List[Pet],
                              petIdsToFetch: List[String],
                              fetching: Boolean,
                              finished: Boolean)
    case class FetchedPets(rId: String,
                           fetchedPets: List[Pet],
                           // freedRequests != fetchedPets.length, since some Pets may be None
                           freedRequests: Int)
    case object Tick

    private var concurrentRequestsInProgress = 0
    private var tasksInProgress = List.empty[TaskInProgress]
    private val schedulerTimeout = 10 millis

    override def receive: Receive = {
      case LoadPets(user) => {
        val rId = UUID.randomUUID().toString
        val newTask = TaskInProgress(rId, sender, user, List(), user.petIds,
          fetching = false, finished = user.petIds.isEmpty)
        tasksInProgress = newTask::tasksInProgress
        log.info(s"[$rId] scheduled new task for user [${user.id}]")
        // start processing
        scheduleTick()
      }

      case FetchedPets(rId, fetchedPets, freedRequests) => {
        log.info(s"[$rId] fetched ${fetchedPets.length} pets")
        val (List(affectedTask), otherTasks) = tasksInProgress.partition(_.rId == rId)
        val taskFetchedPets = affectedTask.fetchedPets ++ fetchedPets
        val processedTask = affectedTask.copy(fetchedPets = taskFetchedPets,
          fetching = false,
          finished = affectedTask.petIdsToFetch.isEmpty)

        tasksInProgress = processedTask::otherTasks
        concurrentRequestsInProgress -= freedRequests
        log.info(s"[$rId] freed $freedRequests request slots; currently using $concurrentRequestsInProgress.")
        scheduleTick()
      }

      case Tick => {
        val (alreadyFetchingTasks, remainingTasks) = tasksInProgress.partition(_.fetching)
        val (finishedTasks, notFinishedTasks) = remainingTasks.partition(_.finished)

        // for all finished tasks respond to senders
        finishedTasks.foreach(task => {
          task.sender ! PetOwnerResponse(PetOwner(task.user, task.fetchedPets))
          log.info(s"[${task.rId}] finished.")
        })

        notFinishedTasks match {
          case Nil => {
            // no tasks to start fetching - just remove already finished
            // no need to schedule another Tick, since we will either get message on fetch
            // or we don't have anything more to process
            tasksInProgress = alreadyFetchingTasks
          }

          case task::remainingNotFinishedTasks => {
            if (concurrentRequestsInProgress < maxConcurrentRequests) {
              // we can fetch some pets now
              val allowedRequests = maxConcurrentRequests - concurrentRequestsInProgress
              val (petIdsToFetchNow, remainingPetIds) = task.petIdsToFetch.splitAt(allowedRequests)
              fetchPets(task.rId, petIdsToFetchNow)

              val remainingTask = task.copy(petIdsToFetch = remainingPetIds, fetching = true)
              concurrentRequestsInProgress += petIdsToFetchNow.length
              log.info(s"[${task.rId}] taken ${petIdsToFetchNow.length} request slots, " +
                s"currently using $concurrentRequestsInProgress.")
              tasksInProgress = remainingTask::(alreadyFetchingTasks ++ remainingNotFinishedTasks)
            } else {
              // we are in rate-limiting mode
              tasksInProgress = notFinishedTasks ++ alreadyFetchingTasks
            }
          }
        }
      }
    }

    private def fetchPets(rId: String,
                          petIdsToFetch: List[String]): Unit = {
      log.info(s"[$rId] fetching ${petIdsToFetch.length} pets")
      Future
        .sequence(petIdsToFetch.map(getPet))
        .map(_.flatten)
        .onComplete {
          case Success(pets) => self ! FetchedPets(rId, pets, petIdsToFetch.length)
          // we don't process failures for simplicity
          // one of approaches would be to store petIds scheduled for fetching in
          // TaskInProgress and retry on failure
          case Failure(_) => log.error(s"[$rId] failed fetching pets [${petIdsToFetch.mkString(", ")}]")
        }
    }

    private def scheduleTick(): Unit = {
      context.system.scheduler.scheduleOnce(schedulerTimeout, self, Tick)
    }
  }

  // alternative solution for the easy case (at max 4 pets per request), using only Futures
  // it's easier than actors/streams in some cases, but the downside:
  // it's not easy to change it for the complex case (at max 4 pet requests at any time)
  def getPetOwnerWithConcurrencyLimit(maxConcurrentRequests: Int)
                                     (userId: String): Future[Option[PetOwner]] = {
    getUser(userId).flatMap({
      case None => {
        Future { None }
      }
      case Some(user) => {
        val subFutures =
          user.petIds
            // this is critical - by splitting collection in sub-collections
            // with at most maxConcurrentRequests we can guarantee that each future
            // will execute not more than maxConcurrentRequests
            .grouped(maxConcurrentRequests)
            .map(petIds => Future.traverse(petIds)(getPet).map(_.flatten))

        // we use this trick with folding and for comprehension
        // to guarantee that we execute subFutures one by one
        val petsFuture = subFutures.foldLeft(Future(List.empty[Pet])) {
          (prevFuture, currFuture) => {
            for {
              prevPetsSubList <- prevFuture
              next <- currFuture
            } yield prevPetsSubList ++ next
          }
        }

        petsFuture.map(pets => Some(PetOwner(user, pets)))
      }
    })
  }
}
