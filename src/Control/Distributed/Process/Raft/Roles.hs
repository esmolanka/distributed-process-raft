{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Control.Distributed.Process.Raft.Roles
  ( leader
  , candidate
  , follower
  ) where

import Control.Monad
import Control.Monad.Reader
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM

import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Reminder

import qualified Data.Map.Strict as M
import Data.Maybe

import Control.Distributed.Process.Raft.Types
import Control.Distributed.Process.Raft.AppendEntriesRpc
import Control.Distributed.Process.Raft.RequestVoteRpc
import Control.Distributed.Process.Raft.ClientRpc
import Control.Distributed.Process.Raft.Utils

leader :: (Show a, Serializable a) => RaftConfig -> TVar (RaftState a) -> Process ()
leader cfg@RaftConfig{..} st = do
  term <- peeks st currentTerm
  say $ "Became leader of term " ++ show term

  leaderId <- getSelfNode
  let otherPeers = filter (/= leaderId) peers

  liftIO $ atomically $ do
    modifyTVar st $ \s ->
      let (_, lastLogIndex) = getLastLogItem s in
      s { nextIndex   = M.fromList $ map (\node -> (node, lastLogIndex)) otherPeers
        , matchIndex  = M.fromList $ map (\node -> (node, 0)) otherPeers
        , clientQueue = M.empty
        }

  self <- getSelfPid
  heartbeat <- spawnLocal $ do
    link self
    forever $ do
      mapM (sendHeartbeat st) peers
      liftIO $ threadDelay (heartbeatRate * 1000)

  newRole <- collectCommits
  exit heartbeat ()

  case newRole of
    Leader -> leader cfg st
    Candidate -> candidate cfg st
    FollowerOf leaderId -> follower cfg st (Just leaderId)

  where
    collectCommits :: Process Role
    collectCommits = do
      leaderId <- getSelfNode
      commit <- receiveWait
        [ match $ \commit@AppendEntriesResp{..} -> do
            (_, _) <- checkCommit commit st peers
            return Nothing

        , match $ \req@AppendEntriesReq{..} -> do
            (_, success) <- appendEntries req st (Just leaderId)
            if success
              then return (Just (FollowerOf areqLeaderId))
              else return Nothing

        , match $ \req@RequestVoteReq{..} -> do
            (_, success) <- voteFor req st (Just leaderId)
            if success
              then return (Just (FollowerOf vreqCandidateId))
              else return Nothing

        , match $ \req@ClientCommitReq{..} -> do
            clientCommit req st (Just leaderId)
            return Nothing

        , matchUnknown (say "Unknown message" >> return Nothing)
        ]
      case commit of
        Nothing -> collectCommits
        Just newRole -> return newRole

candidate :: (Show a, Serializable a) => RaftConfig -> TVar (RaftState a) -> Process ()
candidate cfg@RaftConfig{..} st = do
  term <- peeks st currentTerm
  say $ "Became candidate of term " ++ show term

  currentElectionTimeout <- randomElectionTimeout (electionTimeout * 1000)
  pokes st $ \s -> s { currentTerm = let Term n = currentTerm s in Term (succ n) }
  reminder <- remindAfter currentElectionTimeout ()

  startElection st peers

  candidateId <- getSelfNode

  -- Collect votes from quorum - 1 (voted for self)
  newRole <- collectVotes (quorumCount candidateId peers - 1)
  exit reminder ()
  case newRole of
    Leader -> leader cfg st
    Candidate -> candidate cfg st
    FollowerOf leaderId -> follower cfg st (Just leaderId)

  where
    collectVotes :: Int -> Process Role
    collectVotes n | n <= 0 = return Leader
                   | otherwise = do
      say $ "Awaiting " ++ show n ++ " more votes"
      vote <- receiveWait
        [ match $ \(req :: RequestVoteReq) -> do
           (_, success) <- voteFor req st Nothing
           if success
             then return (Left (FollowerOf (vreqCandidateId req)))
             else return (Right n)

        , match $ \(votingResponse :: RequestVoteResp) -> do
            (_, success) <- checkVote votingResponse st
            if success
              then return (Right (pred n))
              else return (Right n)

        , match $ \(req :: AppendEntriesReq a) -> do
            (_, success) <- appendEntries req st Nothing
            if success
              then return (Left (FollowerOf (areqLeaderId req)))
              else return (Right n)

        , matchIf (isNothing . creqPayload) $ \req@ClientCommitReq{..} -> do
            clientCommit req st Nothing
            return (Right n)

        , match $ \(Reminder ()) -> do
            return (Left Candidate)
        ]
      case vote of
        Left newRole -> return newRole
        Right rem -> collectVotes rem

follower :: (Show a, Serializable a) => RaftConfig -> TVar (RaftState a) -> Maybe NodeId -> Process ()
follower cfg@RaftConfig{..} st votedFor = do
  currentElectionTimeout <- randomElectionTimeout (electionTimeout * 1000)
  res <- receiveTimeout currentElectionTimeout
    [ match $ \req@AppendEntriesReq{..} -> do
        (_, success) <- appendEntries req st votedFor
        if success
          then return (Just areqLeaderId)
          else return votedFor

    , match $ \req@RequestVoteReq{..} -> do
        (_, success) <- voteFor req st votedFor
        if success
          then return (Just vreqCandidateId)
          else return votedFor

    , match $ \req@ClientCommitReq{..} -> do
        clientCommit req st votedFor
        return votedFor

    , matchUnknown (say "Unknown message" >> return votedFor)
    ]
  case res of
    Nothing -> candidate cfg st
    Just newLeader -> follower cfg st newLeader
