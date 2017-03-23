package main
import akka.actor._

import scala.concurrent.duration.DurationInt
import akka.pattern._
import scala.concurrent.Future

import scala.util.Success
import scala.util.Failure
import scala.concurrent.ExecutionContext.Implicits.global
import akka.util.Timeout
import scala.concurrent.TimeoutException

import scala.collection.mutable.ArrayBuffer

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object Merger {
  case class Reply(arr: ArrayBuffer[Int])
  val logger = LoggerFactory.getLogger(classOf[Merger])
}

class Merger extends Actor{
  var mergers: Array[ActorRef] = new Array[ActorRef](2)

  override def preStart(): Unit = {
    Merger.logger.info(self + " initiated")
  }
  def merge(arr1: ArrayBuffer[Int], arr2: ArrayBuffer[Int]): ArrayBuffer[Int] = {
    var mergedArray: ArrayBuffer[Int] = ArrayBuffer()

    for(x <- 1 to arr1.length + arr2.length) {
      if(arr1.nonEmpty && arr2.nonEmpty) {
        if(arr1(0) <= arr2(0)) {
          mergedArray :+= arr1(0)
          arr1.remove(0)
        } else {
          mergedArray :+= arr2(0)
          arr2.remove(0)
        }
      } else if(arr1.isEmpty && arr2.nonEmpty) {
        mergedArray :+= arr2(0)
        arr2.remove(0)
      } else {
        mergedArray :+= arr1(0)
        arr1.remove(0)
      }
    }
    mergedArray
  }

  def receive = {
    case ParentMerger.SendHalf(array: ArrayBuffer[Int]) if array.length == 1 => {
      sender() ! Merger.Reply(array)
    }

    case ParentMerger.SendHalf(array: ArrayBuffer[Int]) if array.length >= 2 => {
      for(i <- 0 to 1) {
        mergers(i) = context.actorOf(Props[Merger])
      }

      implicit var timeout = Timeout(60.seconds)
      var arrayFuture1 = mergers(0) ? ParentMerger.SendHalf(array.slice(0, array.length/2))
      var arrayFuture2 = mergers(1) ? ParentMerger.SendHalf(array.slice(array.length/2, array.length))

      // LESSON OF THE DAY: Don't put sender() inside onComplete blocks. I think in those blocks, sender() refers
      // to the sender of Merger.Reply. Or maybe not. It's a bit random
      val originalSender = sender()
      arrayFuture1.onComplete {
        case Success(Merger.Reply(arr1: ArrayBuffer[Int])) => {
          arrayFuture2.onComplete {
            case Success(Merger.Reply(arr2: ArrayBuffer[Int])) => {
              originalSender ! Merger.Reply(merge(arr1, arr2))
              // Stop all offspring
              context.stop(mergers(0))
              context.stop(mergers(1))
            }
          }
        }
      }
    }
  }
}
