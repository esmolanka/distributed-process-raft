{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Control.Distributed.Process.Raft
  ( startRaft
  , addToLog
  , getLog
  , RaftConfig(..)
  , Raft
  ) where

import System.Random

import Control.Monad
import Control.Monad.Reader
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM

import Control.Distributed.Process
import Control.Distributed.Process.Serializable

import Data.Maybe
import Data.Binary (Binary)
import qualified Data.Map.Strict as M
import Data.Typeable

import GHC.Generics

----------------------------------------------------------------------
-- Interface

data Raft a = Raft ProcessId

startRaft :: forall a. (Serializable a) => RaftConfig -> Process (Raft a)
startRaft cfg = do
  st <- liftIO $ newTVarIO (initRaftState :: RaftState a)
  fmap Raft . spawnLocal $ do
    self <- getSelfPid
    register raftServerName self
    follower cfg st Nothing

addToLog :: Raft a -> a -> Process ()
addToLog _ _ = return ()

getLog :: Raft a -> Process [a]
getLog _ = return []

----------------------------------------------------------------------
-- RPC

type Term = Int

data AppendEntriesReq a = AppendEntriesReq
  { areqTerm         :: Term
  , areqLeaderId     :: NodeId
  , areqPrevLogIndex :: Int
  , areqPrevLogTerm  :: Term
  , areqEntries      :: [a]
  , areqLeaderCommit :: Int
  } deriving (Show, Generic, Typeable)

instance Binary a => Binary (AppendEntriesReq a)

data AppendEntriesResp = AppendEntriesResp
  { arespTerm    :: Term
  , arespSuccess :: Bool
  } deriving (Show, Generic, Typeable)

instance Binary AppendEntriesResp

data RequestVoteReq = RequestVoteReq
  { vreqTerm         :: Term -- candidate's term
  , vreqCandidateId  :: NodeId -- candidate requesting vote
  , vreqLastLogIndex :: Int
  , vreqLastLogTerm  :: Int
  } deriving (Show, Generic, Typeable)

instance Binary RequestVoteReq

data RequestVoteResp = RequestVoteResp
  { vrespTerm        :: Term
  , vrespVoteGranted :: Bool
  } deriving (Show, Generic, Typeable)

instance Binary RequestVoteResp

----------------------------------------------------------------------
-- States

data Role
  = FollowerOf NodeId
  | Candidate
  | Leader

data RaftConfig = RaftConfig
  { electionTimeout :: Int -- milliseconds
  , heartbeatRate   :: Int -- milliseconds
  , peers           :: [NodeId]
  }

data RaftState a = RaftState
  { currentTerm     :: Term
  , currentLog      :: [((Term, Int), a)]
  , commitIndex     :: Int
  , lastApplied     :: Int
  , nextIndex       :: M.Map NodeId Int
  , matchIndex      :: M.Map NodeId Int
  }

initRaftState :: RaftState a
initRaftState = RaftState
  { currentTerm  = 0
  , currentLog   = []
  , commitIndex  = 0
  , lastApplied  = 0
  , nextIndex    = M.empty
  , matchIndex   = M.empty

  }

peeks :: TVar a -> (a -> b) -> Process b
peeks st f = f <$> liftIO (atomically (readTVar st))

pokes :: TVar a -> (a -> a) -> Process ()
pokes st f = liftIO (atomically (modifyTVar st f))



leader :: (Serializable a) => RaftConfig -> TVar (RaftState a) -> Process ()
leader cfg@RaftConfig{..} st = do
  term <- peeks st currentTerm
  say $ "Became leader of term " ++ show term

  leaderId <- getSelfNode
  let otherPeers = filter (/= leaderId) peers

  liftIO $ atomically $ do
    modifyTVar st $ \s ->
      s { nextIndex  = M.fromList  $ map (\node -> (node, lastApplied s)) otherPeers
        , matchIndex = M.fromList $ map (\node -> (node, 0)) otherPeers
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
            checkCommit commit st
            return Nothing

        , match $ \req@AppendEntriesReq{..} -> do
            success <- appendEntries req st (Just leaderId)
            if success
              then return (Just (FollowerOf areqLeaderId))
              else return Nothing

        , match $ \req@RequestVoteReq{..} -> do
            success <- voteFor req st (Just leaderId)
            if success
              then return (Just (FollowerOf vreqCandidateId))
              else return Nothing

        , matchUnknown (return Nothing)
        ]
      case commit of
        Nothing -> collectCommits
        Just newRole -> return newRole



candidate :: (Serializable a) => RaftConfig -> TVar (RaftState a) -> Process ()
candidate cfg@RaftConfig{..} st = do
  term <- peeks st currentTerm
  say $ "Became candidate of term " ++ show term

  currentElectionTimeout <- randomElectionTimeout (electionTimeout * 1000)

  candidateId <- getSelfNode
  snapshot    <- liftIO $ atomically $ do
    modifyTVar st $ \s ->
      s { currentTerm = succ (currentTerm s)
        }
    readTVar st

  let (lastLogTerm, lastLogIndex) =
        case currentLog snapshot of
          [] -> (0, 0)
          (coords, _) : _ -> coords

  let voterequest = RequestVoteReq
        { vreqTerm         = currentTerm snapshot
        , vreqCandidateId  = candidateId
        , vreqLastLogIndex = lastLogIndex
        , vreqLastLogTerm  = lastLogTerm
        }

  reminder <- remindAfter currentElectionTimeout ()

  let otherPeers = filter (/= candidateId) peers

  -- Send voting requests
  mapM (\peer -> sendNode peer voterequest) otherPeers

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
        [ match $ \(votingResponse :: RequestVoteResp) -> do
            success <- checkVote votingResponse st
            if success
              then return (Right (pred n))
              else return (Right n)

        , match $ \(req :: AppendEntriesReq a) -> do
            candidateId <- getSelfNode
            success <- appendEntries req st (Just candidateId)
            if success
              then return (Left (FollowerOf (areqLeaderId req)))
              else return (Right n)

        , match $ \(Reminder ()) -> do
            return (Left Candidate)

        , matchUnknown (return (Right n))
        ]
      case vote of
        Left newRole -> return newRole
        Right rem -> collectVotes rem



follower :: (Serializable a) => RaftConfig -> TVar (RaftState a) -> Maybe NodeId -> Process ()
follower cfg@RaftConfig{..} st votedFor = do
  currentElectionTimeout <- randomElectionTimeout (electionTimeout * 1000)
  res <- receiveTimeout currentElectionTimeout
    [ match $ \req@AppendEntriesReq{..} -> do
        success <- appendEntries req st votedFor
        if success
          then return (Just areqLeaderId)
          else return votedFor

    , match $ \req@RequestVoteReq{..} -> do
        success <- voteFor req st votedFor
        if success
          then return (Just vreqCandidateId)
          else return votedFor
    ]
  case res of
    Nothing -> candidate cfg st
    Just newLeader -> follower cfg st newLeader

----------------------------------------------------------------------
-- RPC handlers

sendHeartbeat :: (Serializable a) => TVar (RaftState a) -> NodeId -> Process ()
sendHeartbeat st node = do
  say $ "Heartbeat to " ++ show node
  term <- peeks st currentTerm
  leaderId <- getSelfNode
  leaderCommit <- peeks st commitIndex
  log <- peeks st currentLog

  sendNode node $ AppendEntriesReq
    { areqTerm         = term
    , areqLeaderId     = leaderId
    , areqPrevLogIndex = 0
    , areqPrevLogTerm  = 0
    , areqEntries      = map snd $ take 0 log
    , areqLeaderCommit = leaderCommit
    }

voteFor :: RequestVoteReq -> TVar (RaftState a) -> Maybe NodeId -> Process Bool
voteFor RequestVoteReq{..} st votedFor = do
  say $ "Voting for " ++ show vreqCandidateId
  term <- peeks st currentTerm
  let granted = term < vreqTerm || isNothing votedFor
  sendNode vreqCandidateId RequestVoteResp{ vrespTerm = term, vrespVoteGranted = granted }
  return granted

checkVote :: RequestVoteResp -> TVar (RaftState a) -> Process Bool
checkVote RequestVoteResp{..} st = do
  say $ "Checking vote"
  pokes st $ \s -> s { currentTerm = max vrespTerm (currentTerm s) }
  return vrespVoteGranted

appendEntries :: AppendEntriesReq a -> TVar (RaftState a) -> Maybe NodeId -> Process Bool
appendEntries AppendEntriesReq{..} st votedFor = do
  say $ "Heartbeat from leader " ++ show areqLeaderId

  success <- liftIO $ atomically $ do
    term <- currentTerm <$> readTVar st
    if term < areqTerm || (term <= areqTerm && votedFor == Just areqLeaderId)
      then do
        modifyTVar st $ \s -> s { currentTerm = max areqTerm term }
        return True
      else
        return False

  return success

checkCommit :: AppendEntriesResp -> TVar (RaftState a) -> Process Bool
checkCommit AppendEntriesResp{..} _ =
  return False

----------------------------------------------------------------------
-- Utils

raftServerName :: String
raftServerName = "raft"

sendNode :: (Serializable a) => NodeId -> a -> Process ()
sendNode nid a = nsendRemote nid raftServerName a

data Reminder a = Reminder a
  deriving (Show, Generic, Typeable)
instance Binary a => Binary (Reminder a)

remindAfter :: (Serializable a) => Int -> a -> Process ProcessId
remindAfter micros payload = do
  pid <- getSelfPid
  spawnLocal $ do
    link pid
    liftIO $ threadDelay micros
    send pid (Reminder payload)

quorumCount :: NodeId -> [NodeId] -> Int
quorumCount me peers =
  (length (filter (/=me) peers) + 2) `div` 2

randomElectionTimeout :: Int -> Process Int
randomElectionTimeout base =
  (*) <$> pure (base `div` 1000)
      <*> liftIO (randomRIO (1000, 2000))
