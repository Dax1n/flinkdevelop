package com.daxin.batch

/**
  * Created by Daxin on 2017/6/16.
  */
trait Dog {
  def run(): Unit

  def eat(): Unit = {
    println("eat ...")
  }

}


class DogImpl extends Dog {
  override def run(): Unit = println("run ....")

  override def eat(): Unit = {
    println("eat ...")
  }
}
