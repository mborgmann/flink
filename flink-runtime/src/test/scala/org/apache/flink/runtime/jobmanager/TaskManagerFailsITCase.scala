/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.flink.runtime.jobmanager

import akka.actor.{Kill, ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit}
import org.apache.flink.runtime.jobgraph.{AbstractJobVertex, DistributionPattern, JobGraph}
import org.apache.flink.runtime.jobmanager.Tasks.{BlockingReceiver, Sender}
import org.apache.flink.runtime.messages.JobManagerMessages.{RequestNumberRegisteredTaskManager,
JobResultFailed, SubmissionSuccess, SubmitJob}
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages._
import org.apache.flink.runtime.testingUtils.TestingUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

@RunWith(classOf[JUnitRunner])
class TaskManagerFailsITCase(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TestingActorSystem", TestingUtils.testConfig))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "The JobManager" should {

    "detect a failing task manager" in {
      val num_slots = 11

      val cluster = TestingUtils.startTestingClusterDeathWatch(num_slots, 2)

      val taskManagers = cluster.getTaskManagers
      val jm = cluster.getJobManager

      try{
        within(TestingUtils.TESTING_DURATION){
          jm ! RequestNumberRegisteredTaskManager
          expectMsg(2)

          jm ! NotifyWhenTaskManagerTerminated(taskManagers(0))

          taskManagers(0) ! PoisonPill

          val TaskManagerTerminated(tm) = expectMsgClass(classOf[TaskManagerTerminated])

          jm ! RequestNumberRegisteredTaskManager
          expectMsg(1)
        }
      }finally{
        cluster.stop()
      }

    }

    "handle gracefully failing task manager" in {
      val num_tasks = 31
      val sender = new AbstractJobVertex("Sender")
      val receiver = new AbstractJobVertex("Receiver")
      sender.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[BlockingReceiver])
      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)
      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise Job", sender, receiver)
      val jobID = jobGraph.getJobID

      val cluster = TestingUtils.startTestingCluster(num_tasks, 2)

      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph)
          expectMsg(SubmissionSuccess(jobGraph.getJobID))

          jm ! WaitForAllVerticesToBeRunningOrFinished(jobID)

          expectMsg(AllVerticesRunning(jobID))

          jm ! RequestWorkingTaskManager(jobID)

          val tm = expectMsgType[WorkingTaskManager].taskManager
          // kill one task manager
          tm ! PoisonPill
          expectMsgType[JobResultFailed]
        }
      }finally{
        cluster.stop()
      }
    }

    "handle hard failing task manager" in {
      val num_tasks = 31
      val sender = new AbstractJobVertex("Sender")
      val receiver = new AbstractJobVertex("Receiver")
      sender.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[BlockingReceiver])
      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)
      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise Job", sender, receiver)
      val jobID = jobGraph.getJobID

      val cluster = TestingUtils.startTestingCluster(num_tasks, 2)

      val taskManagers = cluster.getTaskManagers
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph)
          expectMsg(SubmissionSuccess(jobGraph.getJobID))

          jm ! WaitForAllVerticesToBeRunningOrFinished(jobID)
          expectMsg(AllVerticesRunning(jobID))

          // kill one task manager
          taskManagers(0) ! Kill
          expectMsgType[JobResultFailed]
        }
      }finally{
        cluster.stop()
      }
    }
  }

}
