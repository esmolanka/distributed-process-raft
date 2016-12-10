{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Control.Distributed.Process.Raft.RequestVoteRpc
  ( startElection
  , voteFor
  , checkVote
  ) where

import Control.Monad.Reader
import Control.Concurrent.STM

import Control.Distributed.Process

import Data.Maybe

import Control.Distributed.Process.Raft.Types
import Control.Distributed.Process.Raft.Utils

startElection :: TVar (RaftState a) -> [NodeId] -> Process ()
startElection st peers = do
  s@RaftState{..} <- peeks st id
  let (lastLogTerm, lastLogIndex) = getLastLogItem s
  candidateId <- getSelfNode
  let otherPeers = filter (/= candidateId) peers

  -- Send voting requests
  forM_ otherPeers $ \peer -> do
    request <- issueRpc st RequestVoteRPC $ \reqid ->
      RequestVoteReq
        { vreqRequestId    = reqid
        , vreqTerm         = currentTerm
        , vreqCandidateId  = candidateId
        , vreqLastLogIndex = lastLogIndex
        , vreqLastLogTerm  = lastLogTerm
        }
    sendNode peer request

voteFor :: RequestVoteReq -> TVar (RaftState a) -> Maybe NodeId -> Process (NodeId, Bool)
voteFor RequestVoteReq{..} st votedFor = do
  term <- peeks st currentTerm
  (lastLogTerm, lastLogIndex) <- peeks st getLastLogItem
  let legitimate = term < vreqTerm || isNothing votedFor
      granted    = legitimate && vreqLastLogTerm >= lastLogTerm
                              && vreqLastLogIndex >= lastLogIndex

  if granted
    then say $ "Voting for " ++ show vreqCandidateId
    else say $ "Not voting for " ++ show vreqCandidateId

  self <- getSelfNode

  sendNode vreqCandidateId RequestVoteResp
    { vrespRequestId = vreqRequestId
    , vrespTerm = term
    , vrespNodeId = self
    , vrespVoteGranted = granted
    }

  return (vreqCandidateId, granted)

checkVote :: RequestVoteResp -> TVar (RaftState a) -> Process (NodeId, Bool)
checkVote RequestVoteResp{..} st = do
  pokes st $ \s -> s { currentTerm = max vrespTerm (currentTerm s) }
  req <- lookupRpc st vrespRequestId
  case req of
    Nothing ->
      return (vrespNodeId, False)
    Just _  -> do
      if vrespVoteGranted
        then say "Vote granted"
        else say "Vote not granted"
      return (vrespNodeId, vrespVoteGranted)
