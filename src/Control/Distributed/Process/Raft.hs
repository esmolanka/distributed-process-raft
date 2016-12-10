{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Control.Distributed.Process.Raft
  ( startRaft
  , commit
  , retrieve
  , RaftConfig(..)
  , RaftServer
  ) where

import Control.Monad
import Control.Monad.Reader
import Control.Concurrent.STM

import Control.Distributed.Process
import Control.Distributed.Process.Serializable

import Data.Maybe

import Control.Distributed.Process.Raft.Types
import Control.Distributed.Process.Raft.Roles (follower)


-- | Abstract Raft server type, which represents a handle of Raft
-- consensus service process.
data RaftServer a = Raft ProcessId (TVar Int)

-- | Start Raft consensus server
startRaft :: forall a. (Show a, Serializable a) => RaftConfig -> Process (RaftServer a)
startRaft cfg = do
  st <- liftIO $ newTVarIO (initRaftState :: RaftState a)
  reqCounter <- liftIO $ newTVarIO 0

  pid <- spawnLocal $ do
    self <- getSelfPid
    register raftServerName self
    follower cfg st Nothing

  return $ Raft pid reqCounter

-- | Propose an entry into log. Synchronous. Might timeout after 60 seconds or be rejected.
commit :: forall a. (Serializable a) => RaftServer a -> a -> Process Bool
commit (Raft pid counter) payload = do
  self <- getSelfPid
  reqid <- liftIO $ atomically $ do
    modifyTVar' counter succ
    readTVar counter

  send pid ClientCommitReq
    { creqRequestId = reqid
    , creqClientPid = self
    , creqPayload = Just payload
    , creqRequestLog = False
    }

  success <- receiveTimeout (60 * 1000000)
    [ matchIf ((== reqid) . crespRequestId) $ \(r :: ClientCommitResp a) ->
        return . crespSuccess $ r
    ]
  return $ fromMaybe False success

-- | Retrieve committed log. Synchronous. Timeout: 60 seconds.
retrieve :: forall a. (Serializable a) => RaftServer a -> Process (Maybe [a])
retrieve (Raft pid counter) = do
  self <- getSelfPid
  reqid <- liftIO $ atomically $ do
    modifyTVar' counter succ
    readTVar counter

  send pid ClientCommitReq
    { creqRequestId = reqid
    , creqClientPid = self
    , creqPayload = (Nothing :: Maybe a)
    , creqRequestLog = True
    }

  fmap join $ receiveTimeout (60 * 1000000)
    [ matchIf ((== reqid) . crespRequestId) $ \(r :: ClientCommitResp a) ->
        return . crespLog $ r
    ]
