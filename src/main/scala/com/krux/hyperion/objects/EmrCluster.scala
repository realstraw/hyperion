package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpEmrCluster

trait EmrCluster extends ResourceObject {
  def serialize: AdpEmrCluster
}
