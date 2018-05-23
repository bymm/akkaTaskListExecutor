package opinov8.anyware.actors

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.pipe

import scala.concurrent.Future
import scala.reflect.{ClassTag, classTag}

trait TaskListExecutor extends ActorLogging { this: Actor =>
  type IdleState
  type TaskItem
  type TaskExecutionResult
  implicit val classTagOfTaskItem: ClassTag[TaskItem] = classTag[TaskItem]
  implicit val classTagOfTaskExecutionResult: ClassTag[TaskExecutionResult] = classTag[TaskExecutionResult]

  case class TaskListExecutionDone(taskExecutionResults: Seq[TaskExecutionResult])

  private object Messages {
    object Start
    case class GotTaskList(taskList: Seq[TaskItem])
    case class GotTaskListFailed(t: Throwable)
    object PlaceNextTask
    case class TaskItemDone(taskItem: TaskItem, taskExecutionResult: TaskExecutionResult)
    case class TaskItemFailed(taskItem: TaskItem, t: Throwable)
  }

  def initState: IdleState
  def getTaskList(idleState: IdleState): Future[Seq[TaskItem]]
  def concurrency: Int
  def updateIdleState(currentIdleState: IdleState): IdleState
  def executeTask(taskItem: TaskItem): Future[TaskExecutionResult]

  import context._
  import Messages._

  var startSender: Option[ActorRef] = None

  override def receive: Receive = idle(initState)

  def idle(idleState: IdleState): Receive = {
    case Start => {
      startSender = Some(sender())
      become(waitingTasks(idleState))
      getTaskList(idleState)
        .map(GotTaskList(_))
        .recover { case t: Throwable => GotTaskListFailed(t) }
        .pipeTo(self)
    }
  }

  def waitingTasks(idleState: IdleState): Receive = {
    case GotTaskList(taskList) => {
      become(processTasks(taskList.toList, Seq[TaskItem](), Seq[TaskExecutionResult](), updateIdleState(idleState)))
      for (_ <- 1 to concurrency)
        self ! PlaceNextTask
    }
    case GotTaskListFailed(t) => {
      log.error(t, "Error getting task list")
      become(idle(updateIdleState(idleState)))
    }
  }

  def processTasks(pendingTasks: List[TaskItem], processingTasks: Seq[TaskItem], taskExecutionResults: Seq[TaskExecutionResult], idleState: IdleState): Receive = {
    case PlaceNextTask => {
      if(pendingTasks.isEmpty && processingTasks.isEmpty) {
        startSender map (_ ! TaskListExecutionDone(taskExecutionResults))
        become(idle(idleState))
      } else {
        pendingTasks match {
          case taskItem :: pendingTasksTail => {
            executeTask(taskItem)
              .map(taskExecutionResult => TaskItemDone(taskItem, taskExecutionResult))
              .recover{ case t: Throwable => TaskItemFailed(taskItem, t) }
              .pipeTo(self)
            become(processTasks(pendingTasksTail, processingTasks :+ taskItem, taskExecutionResults, idleState))
          }
          case Nil =>
        }
      }
    }
    case TaskItemDone(taskItem: TaskItem, taskExecutionResult: TaskExecutionResult) => {
      become(processTasks(pendingTasks, processingTasks.filterNot(_ == taskItem), taskExecutionResults :+ taskExecutionResult, idleState))
      self ! PlaceNextTask
    }
    case TaskItemFailed(taskItem: TaskItem, t: Throwable) => {
      log.error(t, s"Error executing task $taskItem")
      become(processTasks(pendingTasks, processingTasks.filterNot(_ == taskItem), taskExecutionResults, idleState))
      self ! PlaceNextTask
    }
  }
}
