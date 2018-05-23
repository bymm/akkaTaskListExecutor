package opinov8.anyware.actors

import akka.actor.{Actor, ActorLogging}
import akka.pattern.pipe
import scala.concurrent.Future
import scala.reflect.{ClassTag, classTag}

trait TaskListExecutor extends ActorLogging { this: Actor =>
  type IdleState
  type TaskItem
  implicit val classTagOfTaskItem: ClassTag[TaskItem] = classTag[TaskItem]

  private object Messages {
    object Start
    case class GotTaskList(taskList: Seq[TaskItem])
    case class GotTaskListFailed(t: Throwable)
    object PlaceNextTask
    case class TaskItemDone(taskItem: TaskItem)
    case class TaskItemFailed(taskItem: TaskItem, t: Throwable)
  }

  def initState: IdleState
  def getTaskList(idleState: IdleState): Future[Seq[TaskItem]]
  def concurrency: Int
  def updateIdleState(currentIdleState: IdleState): IdleState
  def executeTask(taskItem: TaskItem): Future[Unit]

  import context._
  import Messages._

  override def receive: Receive = idle(initState)

  def idle(idleState: IdleState): Receive = {
    case Start => {
      become(waitingTasks(idleState))
      getTaskList(idleState)
        .map(GotTaskList(_))
        .recover { case t: Throwable => GotTaskListFailed(t) }
        .pipeTo(self)
    }
  }

  def waitingTasks(idleState: IdleState): Receive = {
    case GotTaskList(taskList) => {
      become(processTasks(taskList.toList, Seq[TaskItem](), updateIdleState(idleState)))
      for (_ <- 1 to concurrency)
        self ! PlaceNextTask
    }
    case GotTaskListFailed(t) => {
      log.error(t, "Error getting task list")
      become(idle(updateIdleState(idleState)))
    }
  }

  def processTasks(pendingTasks: List[TaskItem], processingTasks: Seq[TaskItem], idleState: IdleState): Receive = {
    case PlaceNextTask => {
      if(pendingTasks.isEmpty && processingTasks.isEmpty) {
        become(idle(idleState))
      } else {
        pendingTasks match {
          case taskItem :: pendingTasksTail => {
            executeTask(taskItem)
              .map(_ => TaskItemDone(taskItem))
              .recover{ case t: Throwable => TaskItemFailed(taskItem, t) }
              .pipeTo(self)
            become(processTasks(pendingTasksTail, processingTasks :+ taskItem, idleState))
          }
          case Nil =>
        }
      }
    }
    case TaskItemDone(taskItem: TaskItem) => {
      become(processTasks(pendingTasks, processingTasks.filterNot(_ == taskItem), idleState))
      self ! PlaceNextTask
    }
    case TaskItemFailed(taskItem: TaskItem, t: Throwable) => {
      log.error(t, s"Error executing task $taskItem")
      become(processTasks(pendingTasks, processingTasks.filterNot(_ == taskItem), idleState))
      self ! PlaceNextTask
    }
  }
}
