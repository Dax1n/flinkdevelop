package com.daxin.stream.cep

/**
  * Created by Daxin on 2017/4/24.
  */
class Alert(var message: String) {


  override def hashCode(): Int = {
    val prime = 31;
    var result = 1;
    result = prime * result + (if (message == null) 0 else message.hashCode())
    result
  }

  override def equals(obj: scala.Any): Boolean = {

    if (this.eq(obj.asInstanceOf[AnyRef]))
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    val other = obj.asInstanceOf[Alert]
    if (message == null) {
      if (other.message != null)
        return false;
    } else if (!message.equals(other.message))
      return false;
    true
  }


  override def toString: String = {

    "Alert [message=" + message + "]"
  }
}

object Alert extends App {

  val a1 = new Alert("1")
  val a2 = new Alert("2")
  val a3 = new Alert("1")

  //  println(a1.eq(a2))//false
  //  println(a1 ==a2) //alse
  //  println(a1.equals(a2))//flase
  //
  //  println(a1.eq(a3))//false
  //  println(a1 ==a3) //true
  //  println(a1.equals(a3))//true
  println(a1.equals(a1))
  println(a1.equals(a3))
  println(a3.equals(a1))

}