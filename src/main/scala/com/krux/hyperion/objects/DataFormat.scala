package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpDataFormat

trait DataFormat extends PipelineObject {
  def serialize: AdpDataFormat
}
