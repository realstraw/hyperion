package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpActivity

trait PipelineActivity extends PipelineObject {

  def groupedBy(client: String): PipelineActivity
  def named(name: String): PipelineActivity

  def dependsOn(activities: PipelineActivity*): PipelineActivity
  def whenMet(preconditions: Precondition*): PipelineActivity

  def onFail(alarms: SnsAlarm*): PipelineActivity
  def onSuccess(alarms: SnsAlarm*): PipelineActivity
  def onLateAction(alarms: SnsAlarm*): PipelineActivity

}
