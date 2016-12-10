{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Control.Distributed.Process.Raft.AppendEntriesRpc
  ( sendHeartbeat
  , appendEntries
  , checkCommit
  ) where

import Control.Monad.Reader
import Control.Concurrent.STM

import Control.Distributed.Process
import Control.Distributed.Process.Serializable

import Data.List
import qualified Data.Map.Strict as M
import Data.Maybe

import Control.Distributed.Process.Raft.Types
import Control.Distributed.Process.Raft.Utils

sendHeartbeat :: (Show a, Serializable a) => TVar (RaftState a) -> NodeId -> Process ()
sendHeartbeat st node = do
  say $ "Heartbeat to " ++ show node
  leaderId <- getSelfNode
  RaftState{..} <- peeks st id

  let currentNextIndex = M.findWithDefault 0 node nextIndex

  let (entriesToSend, restEntries) = partition (\((_, n), _) -> n >= currentNextIndex) currentLog
      (prevLogTerm, prevLogIndex)  = foldr (\(idx, _) _ -> idx) (Term 0, 0) restEntries

  request <- issueRpc st AppendEntriesRPC $ \reqid ->
    AppendEntriesReq
      { areqRequestId    = reqid
      , areqTerm         = currentTerm
      , areqLeaderId     = leaderId
      , areqPrevLogIndex = prevLogIndex
      , areqPrevLogTerm  = prevLogTerm
      , areqEntries      = entriesToSend
      , areqLeaderCommit = commitIndex
      }

  sendNode node request

appendEntries :: (Show a) => AppendEntriesReq a -> TVar (RaftState a) -> Maybe NodeId -> Process (NodeId, Bool)
appendEntries _r@AppendEntriesReq{..} st votedFor = do
  coords <- peeks st getLastLogItem

  success <- liftIO $ atomically $ do
    s <- readTVar st
    let legitimateLeader =
          areqTerm > (currentTerm s) ||
          areqTerm == (currentTerm s) && votedFor == Just areqLeaderId

        (lastLogTerm, lastLogIndex) = getLastLogItem s

        canAppendEntries =
          areqPrevLogTerm  <= lastLogTerm  &&
          areqPrevLogIndex <= lastLogIndex

    if legitimateLeader && canAppendEntries
      then do
        modifyTVar st $ \s ->
          s { currentTerm = max areqTerm (currentTerm s)
            , currentLog  = areqEntries ++ dropWhile (\((_, n), _) -> n > areqPrevLogIndex) (currentLog s)
            , commitIndex = areqLeaderCommit
            }
        return True
      else
        return False

  coords' <- peeks st getLastLogItem

  say $ "Heartbeat from " ++ show areqLeaderId ++ ": " ++ show coords ++ " -> " ++ show coords'

  self <- getSelfNode
  newTerm <- peeks st currentTerm
  sendNode areqLeaderId AppendEntriesResp
    { arespRequestId = areqRequestId
    , arespTerm = newTerm
    , arespNodeId = self
    , arespSuccess = success
    }

  return (areqLeaderId, success)

checkCommit :: AppendEntriesResp -> TVar (RaftState a) -> [NodeId] -> Process (NodeId, Bool)
checkCommit AppendEntriesResp{..} st peers = do
  request <- lookupRpc st arespRequestId
  leaderId <- getSelfNode
  let quorum = quorumCount leaderId peers - 1

  success <- case request of
    Just (AppendEntriesRPC req) -> do
      liftIO $ atomically $ do
        let lastAppliedIndex :: Maybe Int
            lastAppliedIndex = snd . fst <$> listToMaybe (areqEntries req)

            modifyNextIndex idx =
              if arespSuccess
              then maybe idx succ lastAppliedIndex
              else max 0 (pred idx)

            modifyMatchIndex idx =
              if arespSuccess
              then fromMaybe idx lastAppliedIndex
              else idx

        modifyTVar' st $ \s ->
          let
            newNextIndex  = M.adjust modifyNextIndex arespNodeId (nextIndex s)
            newMatchIndex = M.adjust modifyMatchIndex arespNodeId (matchIndex s)
            quorumCommitIndex =
              max
                (commitIndex s)
                (last . take quorum . sortBy (\b a -> a `compare` b) . M.elems $ newMatchIndex)

          in s { nextIndex  = newNextIndex
               , matchIndex = newMatchIndex
               , commitIndex = quorumCommitIndex
               }

        return (arespSuccess)
    Just  _ -> return False
    Nothing -> return False

  commit <- peeks st commitIndex

  lookupClients st >>= sequence

  if success
    then say $ "Successful append from follower, new commit: " ++ show commit
    else say "Unsuccessful append from follower"
  return (arespNodeId, success)
