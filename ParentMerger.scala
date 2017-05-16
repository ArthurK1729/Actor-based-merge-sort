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

import scala.util.Random

import scala.collection.mutable.ArrayBuffer

object ParentMerger {
  case object Begin
  case class SendHalf(arr: ArrayBuffer[Int])
}

class ParentMerger extends Actor {
  var system = ActorSystem("Mergers")
  var mergers: Array[ActorRef] = new Array[ActorRef](2)
  var array: ArrayBuffer[Int] = new ArrayBuffer[Int](10)

  def merge(arr1: ArrayBuffer[Int], arr2: ArrayBuffer[Int]): ArrayBuffer[Int] = {
    var mergedArray: ArrayBuffer[Int] = ArrayBuffer()
    
    // Refactor to only state the implementation for merging once
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

  override def preStart(): Unit = {
    val r = Random
    array = ArrayBuffer(8)

    for(x <- 1 to 1000) {
      array +:= r.nextInt
    }

    for(i <- 0 to 1) {
      mergers(i) = context.actorOf(Props[Merger], "merger" + i)
    }
    
    context.watch(mergers(0))
    context.watch(mergers(1))

    self ! ParentMerger.Begin
  }

  def receive = {
    case Terminated(actor1) => println("They killed merger1")
    case Terminated(actor2) => println("They killed merger2")
    case ParentMerger.Begin => {
      implicit var timeout = Timeout(60.seconds)
      
      // Assumption: at the beginning the array size is 2 or greater
      var arrayFuture1 = mergers(0) ? ParentMerger.SendHalf(array.slice(0, array.length/2))
      var arrayFuture2 = mergers(1) ? ParentMerger.SendHalf(array.slice(array.length/2, array.length))

      arrayFuture1.onComplete {
        case Success(Merger.Reply(arr1: ArrayBuffer[Int])) => {
          arrayFuture2.onComplete {
            case Success(Merger.Reply(arr2: ArrayBuffer[Int])) => {
              print(merge(arr1, arr2).toString())
              context.stop(mergers(0))
              context.stop(mergers(1))
              context.stop(self)
              // The stop message is propagated to all the children
            }
          }
        }
      }
    }
  }
}
