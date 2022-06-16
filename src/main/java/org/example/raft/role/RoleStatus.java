package org.example.raft.role;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public class RoleStatus {

  public static final int LEADER = 1;

  public static final int FOLLOWER = 2;

  public static final int CANDIDATE = 3;

  public static final int LEARNER = 4;

  /**
   * node  status
   */
  private  volatile int nodeStatus = 2;


  public  boolean leaderToFollower() {
    if (nodeStatus == LEADER) {
      synchronized (RoleStatus.class) {
        if (nodeStatus == LEADER) {
          nodeStatus = FOLLOWER;
          return true;
        }
      }
    }
    return false;
  }

  public  void followerToCandidate() {
    if (nodeStatus == FOLLOWER) {
      synchronized (RoleStatus.class) {
        if (nodeStatus == FOLLOWER) {
          nodeStatus = CANDIDATE;
        }
      }
    }
  }

  public  boolean candidateToFollower() {
    if (nodeStatus == CANDIDATE) {
      synchronized (RoleStatus.class) {
        if (nodeStatus == CANDIDATE) {
          nodeStatus = FOLLOWER;
          return true;
        }
      }
    }
    return false;
  }
  public  void candidateToLeader() {
    if (nodeStatus == CANDIDATE) {
      synchronized (RoleStatus.class) {
        if (nodeStatus == CANDIDATE) {
          nodeStatus = LEADER;
        }
      }
    }
  }

  public  int getNodeStatus() {
    return nodeStatus;
  }
}
