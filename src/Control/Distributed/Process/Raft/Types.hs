{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE RecordWildCards     #-}

module Control.Distributed.Process.Raft.Types
  ( Term (..)
  , AppendEntriesReq (..)
  , AppendEntriesResp (..)
  , RequestVoteReq (..)
  , RequestVoteResp (..)
  , ClientCommitReq (..)
  , ClientCommitResp (..)
  , RaftState (..)
  , RaftConfig (..)
  , RaftRequest (..)
  , Role (..)
  , raftServerName
  , initRaftState
  , getLastLogItem
  , peeks
  , pokes
  , issueRpc
  , lookupRpc
  , lookupClients
  ) where

import Control.Concurrent.STM
import Control.Distributed.Process

import Data.Binary (Binary)
import qualified Data.Map.Strict as M
import Data.Time
import Data.Typeable
import GHC.Generics


newtype Term = Term Int
  deriving (Eq, Ord, Show, Generic, Typeable)

instance Binary Term

data AppendEntriesReq a = AppendEntriesReq
  { areqRequestId    :: Int
  , areqTerm         :: Term
  , areqLeaderId     :: NodeId
  , areqPrevLogIndex :: Int
  , areqPrevLogTerm  :: Term
  , areqEntries      :: [((Term, Int), a)]
  , areqLeaderCommit :: Int
  } deriving (Show, Generic, Typeable)

instance (Binary a) => Binary (AppendEntriesReq a)

data AppendEntriesResp = AppendEntriesResp
  { arespRequestId   :: Int
  , arespTerm        :: Term
  , arespNodeId      :: NodeId
  , arespSuccess     :: Bool
  } deriving (Show, Generic, Typeable)

instance Binary AppendEntriesResp

data RequestVoteReq = RequestVoteReq
  { vreqRequestId    :: Int
  , vreqTerm         :: Term
  , vreqCandidateId  :: NodeId
  , vreqLastLogIndex :: Int
  , vreqLastLogTerm  :: Term
  } deriving (Show, Generic, Typeable)

instance Binary RequestVoteReq

data RequestVoteResp = RequestVoteResp
  { vrespRequestId   :: Int
  , vrespTerm        :: Term
  , vrespNodeId      :: NodeId
  , vrespVoteGranted :: Bool
  } deriving (Show, Generic, Typeable)

instance Binary RequestVoteResp

data ClientCommitReq a = ClientCommitReq
  { creqRequestId    :: Int
  , creqClientPid    :: ProcessId
  , creqPayload      :: Maybe a
  , creqRequestLog   :: Bool
  } deriving (Show, Generic, Typeable)

instance (Binary a) => Binary (ClientCommitReq a)

data ClientCommitResp a = ClientCommitResp
  { crespRequestId   :: Int
  , crespLog         :: Maybe [a]
  , crespCommitIndex :: Int
  , crespSuccess     :: Bool
  } deriving (Show, Generic, Typeable)

instance (Binary a) => Binary (ClientCommitResp a)

data RaftRequest a
  = AppendEntriesRPC (AppendEntriesReq a)
  | RequestVoteRPC RequestVoteReq

----------------------------------------------------------------------
-- State

data Role
  = FollowerOf NodeId
  | Candidate
  | Leader

data RaftState a = RaftState
  { pendingRpc      :: !(M.Map Int (UTCTime, RaftRequest a))
  , rpcCounter      :: !Int
  , currentTerm     :: !Term
  , currentLog      :: ![((Term, Int), a)]
  , commitIndex     :: !Int
  , nextIndex       :: !(M.Map NodeId Int)
  , matchIndex      :: !(M.Map NodeId Int)
  , clientQueue     :: !(M.Map Int [Process ()])
  }

data RaftConfig = RaftConfig
  { electionTimeout :: Int      -- ^ Election timeout in milliseconds
  , heartbeatRate   :: Int      -- ^ Heartbeat rate in milliseconds
  , peers           :: [NodeId] -- ^ Known peers
  }


----------------------------------------------------------------------
-- Raft type manipulation helpers

raftServerName :: String
raftServerName = "raft"

initRaftState :: RaftState a
initRaftState = RaftState
  { pendingRpc   = M.empty
  , rpcCounter   = 0
  , currentTerm  = Term 0
  , currentLog   = []
  , commitIndex  = 0
  , nextIndex    = M.empty
  , matchIndex   = M.empty
  , clientQueue  = M.empty
  }

getLastLogItem :: RaftState a -> (Term, Int)
getLastLogItem = foldr (\(coords, _) _-> coords) (Term 0, 0) . currentLog

peeks :: TVar a -> (a -> b) -> Process b
peeks st f = f <$> liftIO (atomically (readTVar st))

pokes :: TVar a -> (a -> a) -> Process ()
pokes st f = liftIO (atomically (modifyTVar' st f))

issueRpc  :: TVar (RaftState a) -> (r -> RaftRequest a) -> (Int -> r) -> Process r
issueRpc st wrap mkReq = do
  now <- liftIO $ getCurrentTime
  liftIO $ atomically $ do
    reqid <- rpcCounter <$> readTVar st
    let request = mkReq reqid
    modifyTVar' st $ \s ->
      s { rpcCounter = succ (rpcCounter s)
        , pendingRpc = M.insert (rpcCounter s) (now, wrap request) (M.filter (onlyNew now) (pendingRpc s))
        }
    return request
  where
    onlyNew :: UTCTime -> (UTCTime, a) -> Bool
    onlyNew now (timestamp, _) = now `diffUTCTime` timestamp < 60

lookupRpc :: TVar (RaftState a) -> Int -> Process (Maybe (RaftRequest a))
lookupRpc st reqId =
  liftIO $ atomically $ do
    req <- M.lookup reqId . pendingRpc <$> readTVar st
    modifyTVar' st (\s -> s { pendingRpc = M.delete reqId (pendingRpc s) })
    return $ fmap snd req

lookupClients :: TVar (RaftState a) -> Process [Process ()]
lookupClients st =
  liftIO $ atomically $ do
    RaftState{..} <- readTVar st
    let (ready, retained) = M.partitionWithKey (\k _ -> k <= commitIndex) clientQueue
    modifyTVar' st (\s -> s { clientQueue = retained })
    return $ concat $ M.elems ready
