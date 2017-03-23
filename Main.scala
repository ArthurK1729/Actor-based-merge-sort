import main.ParentMerger

object Main {
  def main(args: Array[String]): Unit = {
    akka.Main.main(Array(classOf[ParentMerger].getName))
  }
}