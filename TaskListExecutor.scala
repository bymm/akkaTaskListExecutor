package opinov8.anyware.actors

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.pipe
import scala.concurrent.Future

object TaskListExecutor {
  object Start
}

trait TaskListExecutor extends ActorLogging { this: Actor =>
  type IdleState
  type TaskItem
  type TaskExecutionResult

  case class TaskListExecutionDone(taskExecutionResults: Seq[TaskExecutionResult])

  private case class GotTaskList(taskList: Seq[TaskItem])
  private case class GotTaskListFailed(t: Throwable)
  private object PlaceNextTask
  private case class TaskItemDone(taskExecutionResult: TaskExecutionResult)
  private case class TaskItemFailed(t: Throwable)

  def initState: IdleState
  def getTaskList(idleState: IdleState): Future[Seq[TaskItem]]
  def concurrency: Int
  def updateIdleState(currentIdleState: IdleState): IdleState
  def executeTask(taskItem: TaskItem): Future[TaskExecutionResult]

  import context._

  var startSender: Option[ActorRef] = None

  override def receive: Receive = idle(initState)

  def idle(idleState: IdleState): Receive = {
    case TaskListExecutor.Start => {
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
      become(processTasks(taskList.toList, 0, Seq[TaskExecutionResult](), updateIdleState(idleState)))
      for (_ <- 1 to concurrency)
        self ! PlaceNextTask
    }
    case GotTaskListFailed(t) => {
      log.error(t, "Error getting task list")
      become(idle(updateIdleState(idleState)))
    }
  }

  def processTasks(pendingTasks: List[TaskItem], processingTasks: Int, taskExecutionResults: Seq[TaskExecutionResult], idleState: IdleState): Receive = {
    case PlaceNextTask => {
      pendingTasks match {
        case taskItem :: pendingTasksTail => {
          become(processTasks(pendingTasksTail, processingTasks + 1, taskExecutionResults, idleState))
          executeTask(taskItem)
            .map(TaskItemDone(_))
            .recover{ case t: Throwable => TaskItemFailed(t) }
            .pipeTo(self)
        }
        case Nil => {
          if(processingTasks <= 0) {
            startSender map (_ ! TaskListExecutionDone(taskExecutionResults))
            become(idle(idleState))
          }
        }
      }
    }
    case TaskItemDone(taskExecutionResult) => {
      become(processTasks(pendingTasks, processingTasks - 1, taskExecutionResults :+ taskExecutionResult, idleState))
      self ! PlaceNextTask
    }
    case TaskItemFailed(t) => {
      log.error(t, s"Error executing task")
      become(processTasks(pendingTasks, processingTasks - 1, taskExecutionResults, idleState))
      self ! PlaceNextTask
    }
  }
}
