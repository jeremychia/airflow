/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.airflow.sdk.internal

import org.apache.airflow.sdk.Arg
import org.apache.airflow.sdk.DagDef
import org.apache.airflow.sdk.TaskDef
import org.apache.airflow.sdk.TaskGroupRef
import org.apache.airflow.sdk.TaskRef

/**
 * @suppress
 *
 * The recorder behind a `@Builder.Deps` wiring class. Public so that
 * processor-generated wiring views can call it; not user-facing API.
 *
 * A wiring view's methods are `default` methods on an interface, so they hold
 * no Dag of their own. [record] puts the Dag being built in scope for exactly
 * the duration of one `depends()` call, and the view's methods register into
 * it. Nothing parses a syntax tree: identity travels with the [TaskRef] a
 * call returns, so a result held in a local and reused just works.
 */
object Refs {
  private class Recording(
    val dag: DagDef,
    val groupOf: Map<String, String>,
  ) {
    val byTaskId = linkedMapOf<String, TaskRef<*>>()
  }

  private val recording = ThreadLocal<Recording?>()

  /**
   * Runs one `depends()` call with [dag] in scope, then returns the Dag the
   * wiring built.
   *
   * @throws IllegalArgumentException if the wiring left a declared task
   *    unregistered.
   */
  @JvmStatic
  fun record(
    dag: DagDef,
    taskIds: List<String>,
    depends: Runnable,
  ): DagDef = record(dag, taskIds, emptyList(), emptyMap(), depends)

  /**
   * Runs one `depends()` call as [record] does, for a Dag whose tasks sit in
   * task groups.
   *
   * @param groupIds Full ID of every task group, parents before the groups
   *    nested in them. All are created before `depends()` runs, so the wiring
   *    can order a group before calling any of its tasks, and a group holding
   *    no tasks still exists.
   * @param groupOf The group each grouped task belongs to, task ID to full
   *    group ID.
   */
  @JvmStatic
  fun record(
    dag: DagDef,
    taskIds: List<String>,
    groupIds: List<String>,
    groupOf: Map<String, String>,
    depends: Runnable,
  ): DagDef {
    check(recording.get() == null) { "Dag wiring is already being recorded on this thread" }
    groupIds.forEach { ensureGroup(dag, it) }
    recording.set(Recording(dag, groupOf))
    try {
      depends.run()
    } finally {
      recording.remove()
    }
    val missing = taskIds.filterNot { it in dag.tasks }
    require(missing.isEmpty()) {
      "Wiring for Dag '${dag.id}' did not register task(s) ${missing.joinToString { "'$it'" }}: " +
        "every @Builder.Task method must be called in the @Builder.Deps class"
    }
    return dag
  }

  /**
   * Records a task that takes no data arguments.
   *
   * @return The handle representing this task, memoized by [TaskDef.id] so every call
   *    yields the same one.
   */
  @JvmStatic
  fun <T> node(def: TaskDef): TaskRef<T> = call(def)

  /**
   * Records a task and the data edge for every [TaskRef] among [args]; a
   * literal argument records a baked value and no edge.
   *
   * @return The handle representing this task, memoized by [TaskDef.id] so a result
   *    held in a local and reused refers to one node.
   * @throws IllegalArgumentException if an argument is a raw Java `null`
   *    rather than `lit(null)`.
   */
  @JvmStatic
  @Suppress("SpreadOperator")
  fun <T> call(
    def: TaskDef,
    vararg args: Arg<*>?,
  ): TaskRef<T> = call(def, emptyList(), *args)

  /**
   * Records a task called with named arguments, as a generated wiring view
   * does. [names] is the task parameter each argument feeds, in the same
   * order, and is what the serialized Dag carries as the task's binding spec.
   *
   * @return The handle representing this task, memoized by [TaskDef.id] so a result
   *    held in a local and reused refers to one node.
   * @throws IllegalArgumentException if an argument is a raw Java `null`
   *    rather than `lit(null)`.
   */
  @JvmStatic
  @Suppress("UNCHECKED_CAST", "SpreadOperator")
  fun <T> call(
    def: TaskDef,
    names: List<String>,
    vararg args: Arg<*>?,
  ): TaskRef<T> {
    require(names.isEmpty() || names.size == args.size) {
      "Task '${def.id}' was wired with ${args.size} argument(s) under ${names.size} name(s)"
    }
    val inputs =
      args.mapIndexed { i, arg ->
        requireNotNull(arg) {
          "Argument ${i + 1} of task '${def.id}' is null; wrap a null constant as lit(null)"
        }
      }
    val active =
      checkNotNull(recording.get()) {
        "Task '${def.id}' was wired outside a @Builder.Deps class; the wiring view's methods " +
          "only record while the generated builder is running depends()"
      }
    active.byTaskId[def.id]?.let { existing ->
      require(inputs.isEmpty()) {
        "Task '${def.id}' is wired more than once with arguments; call it once and reuse the handle it returned"
      }
      return existing as TaskRef<T>
    }
    inputs.filterIsInstance<TaskRef<*>>().forEach { def.dependsOn(it.def) }
    def.inputs += inputs
    def.inputNames += names
    val group = active.groupOf[def.id]
    if (group == null) {
      active.dag.addTask(def)
    } else {
      active.dag.groups
        .getValue(group)
        .adopt(def)
    }
    return TaskRef<T>(def).also { active.byTaskId[def.id] = it }
  }

  /**
   * The task group with this full ID in the Dag being recorded. Public so the
   * generated wiring view can resolve the group it stands for.
   */
  @JvmStatic
  fun group(id: String): TaskGroupRef {
    val active =
      checkNotNull(recording.get()) {
        "Task group '$id' was looked up outside a @Builder.Deps class"
      }
    return requireNotNull(active.dag.groups[id]) {
      "Dag '${active.dag.id}' has no task group '$id'"
    }
  }

  /** The group with full ID [id] in [dag], created with every enclosing group it is missing. */
  private fun ensureGroup(
    dag: DagDef,
    id: String,
  ): TaskGroupRef {
    dag.groups[id]?.let { return it }
    val parentId = id.substringBeforeLast('.', "")
    val parent = if (parentId.isEmpty()) null else ensureGroup(dag, parentId)
    return if (parent == null) dag.taskGroup(id) else parent.taskGroup(id.substringAfterLast('.'))
  }
}
