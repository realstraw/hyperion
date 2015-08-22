package com.krux.hyperion

import com.krux.hyperion.activity.PipelineActivity
import com.krux.hyperion.common.PipelineObjectId
import scala.collection.mutable.Buffer
import scala.language.implicitConversions

class WorkflowGraph private (
  flow: Map[PipelineObjectId, Set[PipelineObjectId]],
  activities: Map[PipelineObjectId, PipelineActivity],
  roots: Set[PipelineObjectId]
) {

  def this() =
    this(
      Map.empty[PipelineObjectId, Set[PipelineObjectId]],
      Map.empty[PipelineObjectId, PipelineActivity],
      Set.empty[PipelineObjectId]
    )

  implicit def pipelineId2Activity(pId: PipelineObjectId): PipelineActivity = activities(pId)

  def +(act1: PipelineActivity, act2: PipelineActivity) = {
    val dependents = flow.get(act1.id) match {
      case Some(acts) => acts + act2.id
      case None => Set(act2.id)
    }
    val newRoots = roots - act2.id

    val newActivities = activities + (act1.id -> act1) + (act2.id -> act2)

    new WorkflowGraph(flow + (act1.id -> dependents), newActivities, newRoots)
  }

  def toActivities: Iterable[PipelineActivity] = {

    val newFlow = flow -- roots

    if (newFlow.size == 0) {
      activities.values
    } else {

      // get the immediate dependencies from the root node
      val dependencies: Set[(PipelineObjectId, PipelineObjectId)] =
        for { act <- roots; dependent <- flow(act) } yield {
          (dependent, act)
        }

      val actsWithDeps = dependencies.groupBy(_._1)
        .map { case (dependent, group) =>
          dependent.dependsOn(group.map(_._2).toSeq.map(activities): _*)
        }

      // remove the root nodes from activities and update the dependsOn for the new roots
      val newActivities = actsWithDeps
        .foldLeft(activities -- roots)((acts, act) => acts + (act.id -> act))

      val newRoots = actsWithDeps.map(_.id).toSet

      roots.map(activities) ++ (new WorkflowGraph(newFlow, newActivities, newRoots)).toActivities
    }
  }

}
