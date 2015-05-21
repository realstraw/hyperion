package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.Ec2Resource

/**
 * Runs a command or script
 */
case class ShellCommandActivity private (
  id: PipelineObjectId,
  runsOn: Ec2Resource,
  commandOrScriptUri: Either[String, String],
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

  def withCommand(cmd: String) = this.copy(commandOrScriptUri = Left(cmd))
  def withScriptUri(uri: String) = this.copy(commandOrScriptUri = Right(uri))
  def withArguments(args: String*) = this.copy(scriptArguments = scriptArguments ++ args)

  def staged() = this.copy(stage = true)
  def notStaged() = this.copy(stage = false)

  def withInput(inputs: S3DataNode*) = this.copy(input = input ++ inputs)
  def withOutput(outputs: S3DataNode*) = this.copy(output = output ++ outputs)

  def withStdoutTo(out: String) = this.copy(stdout = Option(out))
  def withStderrTo(err: String) = this.copy(stderr = Option(err))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ preconditions ++ input ++ output ++ dependsOn ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = commandOrScriptUri match {
      case Left(command) => Some(command)
      case _ => None
    },
    scriptUri = commandOrScriptUri match {
      case Right(scriptUri) => Some(scriptUri)
      case _ => None
    },
    scriptArgument = scriptArguments,
    input = seqToOption(input)(_.ref),
    output = seqToOption(output)(_.ref),
    stage = stage.toString(),
    stdout = stdout,
    stderr = stderr,
    runsOn = runsOn.ref,
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref)
  )
}

object ShellCommandActivity extends RunnableObject {
  def apply(runsOn: Ec2Resource) =
    new ShellCommandActivity(
      id = PipelineObjectId("ShellCommandActivity"),
      runsOn = runsOn,
      commandOrScriptUri = Left(""),
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