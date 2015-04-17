package com.krux.hyperion.objects

import aws.{AdpJsonSerializer, AdpShellCommandActivity, AdpRef,
  AdpDataNode, AdpActivity, AdpEc2Resource, AdpPrecondition}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpSnsAlarm

trait GoogleStorageActivity extends PipelineActivity

/**
 * Google Storage Download activity
 */
case class GoogleStorageDownloadActivity private (
  id: PipelineObjectId,
  runsOn: Ec2Resource,
  input: String,
  output: Option[S3DataNode],
  botoConfigUrl: String,
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm]
)(
  implicit val hc: HyperionContext
) extends GoogleStorageActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withBotoConfigUrl(url: String) = this.copy(botoConfigUrl = url)
  def withInput(path: String) = this.copy(input = path)
  def withOutput(out: S3DataNode) = this.copy(output = Some(out))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = Some(id),
    command = None,
    scriptUri = Some(s"${hc.scriptUri}gsutil/gsutil_download.sh"),
    scriptArgument = Some(Seq(botoConfigUrl, input)),
    input = None,
    output = output.map(out => Seq(AdpRef(out.serialize))),
    stage = "true",
    stdout = None,
    stderr = None,
    runsOn = AdpRef(runsOn.serialize),
    dependsOn = seqToOption(dependsOn)(act => AdpRef(act.serialize)),
    precondition = seqToOption(preconditions)(precondition => AdpRef(precondition.serialize)),
    onFail = seqToOption(onFailAlarms)(alarm => AdpRef(alarm.serialize)),
    onSuccess = seqToOption(onSuccessAlarms)(alarm => AdpRef(alarm.serialize)),
    onLateAction = seqToOption(onLateActionAlarms)(alarm => AdpRef(alarm.serialize))
  )

}

object GoogleStorageDownloadActivity {
  def apply(runsOn: Ec2Resource)(implicit hc: HyperionContext) =
    new GoogleStorageDownloadActivity(
      id = PipelineObjectId("GoogleStorageDownloadActivity"),
      runsOn = runsOn,
      input = "",
      output = None,
      botoConfigUrl = "",
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )
}

/**
 * Google Storage Upload activity
 */
case class GoogleStorageUploadActivity private (
  id: PipelineObjectId,
  runsOn: Ec2Resource,
  input: Option[S3DataNode],
  output: String,
  botoConfigUrl: String,
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm]
)(
  implicit val hc: HyperionContext
) extends GoogleStorageActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withBotoConfigUrl(url: String) = this.copy(botoConfigUrl = url)
  def withInput(in: S3DataNode) = this.copy(input = Some(in))
  def withOutput(path: String) = this.copy(output = path)

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ dependsOn

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = Some(id),
    command = None,
    scriptUri = Some(s"${hc.scriptUri}gsutil/gsutil_upload.sh"),
    scriptArgument = Some(Seq(botoConfigUrl, output)),
    input = input.map(in => Seq(AdpRef(in.serialize))),
    output = None,
    stage = "true",
    stdout = None,
    stderr = None,
    dependsOn = seqToOption(dependsOn)(act => AdpRef(act.serialize)),
    runsOn = AdpRef(runsOn.serialize),
    precondition = seqToOption(preconditions)(precondition => AdpRef(precondition.serialize)),
    onFail = seqToOption(onFailAlarms)(alarm => AdpRef(alarm.serialize)),
    onSuccess = seqToOption(onSuccessAlarms)(alarm => AdpRef(alarm.serialize)),
    onLateAction = seqToOption(onLateActionAlarms)(alarm => AdpRef(alarm.serialize))
  )

}

object GoogleStorageUploadActivity {
  def apply(runsOn: Ec2Resource)(implicit hc: HyperionContext) =
    new GoogleStorageUploadActivity(
      id = PipelineObjectId("GoogleStorageUploadActivity"),
      runsOn = runsOn,
      input = None,
      output = "",
      botoConfigUrl = "",
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )
}
