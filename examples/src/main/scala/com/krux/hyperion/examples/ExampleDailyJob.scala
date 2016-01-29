package com.krux.hyperion.examples

object ExampleDailyJob extends DataPipelineDef with HyperionCli {

  val schedule = Schedule.cron.startTodayAt(1, 0, 0).every(1.day)

  def workflow = ???

}
