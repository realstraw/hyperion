package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpDatabase

trait Database extends PipelineObject {
  def serialize: AdpDatabase
}
