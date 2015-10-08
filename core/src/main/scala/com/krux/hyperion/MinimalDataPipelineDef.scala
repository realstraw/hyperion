package com.krux.hyperion

import com.krux.hyperion.parameter.Parameter

case class MinimalDataPipelineDef(
  override val pipelineName: String,
  schedule: Schedule,
  workflow: WorkflowExpression,
  override val parameters: Iterable[Parameter[_]]
) extends DataPipelineDef
