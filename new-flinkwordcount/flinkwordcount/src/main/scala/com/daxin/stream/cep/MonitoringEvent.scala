package com.daxin.stream.cep

/**
  * Created by Daxin on 2017/4/24.
  */
class MonitoringEvent(var machineName: String) {

  override def equals(obj: scala.Any): Boolean = {

    if (this.eq(obj.asInstanceOf[AnyRef])) {
      return true
    }
    if (obj == null) {
      return false
    }
    if (getClass() != obj.getClass()) {
      return false
    }
    val other = obj.asInstanceOf[MonitoringEvent]
    if (machineName == null) {
      if (other.machineName != null)
        return false
    } else if (!machineName.equals(other.machineName))
      return false
    return true
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if (machineName == null) 0 else machineName.hashCode());
    result
  }
}
