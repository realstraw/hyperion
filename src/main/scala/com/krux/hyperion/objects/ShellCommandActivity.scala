package com.krux.hyperion.objects

import aws.{AdpJsonSerializer, AdpShellCommandActivity, AdpRef,
  AdpDataNode, AdpActivity, AdpEc2Resource, AdpPrecondition}
import com.krux.hyperion.objects.aws.AdpSnsAlarm

/**
 * Shell command activity
 */
case class ShellCommandActivity private (
  id: PipelineObjectId,
  runsOn: Ec2Resource,
  command: Option[String],
  scriptUri: Option[String],
  scriptArguments: Seq[String],
  stage: Boolean,
  input: Seq[S3DataNode],
  output: Seq[S3DataNode],
  stdout: Option[String],
  stderr: Option[String],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withCommand(cmd: String) = this.copy(command = Some(cmd))
  def withScriptUri(uri: String) = this.copy(scriptUri = Some(uri))
  def withArguments(args: String*) = this.copy(scriptArguments = args)

  def staged() = this.copy(stage = true)
  def notStaged() = this.copy(stage = false)

  def withInput(inputs: S3DataNode*) = this.copy(input = inputs)
  def withOutput(outputs: S3DataNode*) = this.copy(output = outputs)

  def withStdoutTo(out: String) = this.copy(stdout = Some(out))
  def withStderrTo(err: String) = this.copy(stderr = Some(err))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ preconditions ++ input ++ output ++ dependsOn ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpShellCommandActivity(
    id = id,
    name = Some(id),
    command = command,
    scriptUri = scriptUri,
    scriptArgument = scriptArguments,
    input = toOption(input)(in => AdpRef[AdpDataNode](in.id)),
    output = toOption(output)(out => AdpRef[AdpDataNode](out.id)),
    stage = stage.toString(),
    stdout = stdout,
    stderr = stderr,
    runsOn = AdpRef[AdpEc2Resource](runsOn.id),
    dependsOn = toOption(dependsOn)(act => AdpRef[AdpActivity](act.id)),
    precondition = toOption(preconditions)(precondition => AdpRef[AdpPrecondition](precondition.id)),
    onFail = toOption(onFailAlarms)(alarm => AdpRef[AdpSnsAlarm](alarm.id)),
    onSuccess = toOption(onSuccessAlarms)(alarm => AdpRef[AdpSnsAlarm](alarm.id)),
    onLateAction = toOption(onLateActionAlarms)(alarm => AdpRef[AdpSnsAlarm](alarm.id))
  )
}

object ShellCommandActivity {
  def apply(runsOn: Ec2Resource) =
    new ShellCommandActivity(
      id = PipelineObjectId("ShellCommandActivity"),
      runsOn = runsOn,
      command = None,
      scriptUri = None,
      scriptArguments = Seq(),
      stage = true,
      input = Seq(),
      output = Seq(),
      stdout = None,
      stderr = None,
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )
}
