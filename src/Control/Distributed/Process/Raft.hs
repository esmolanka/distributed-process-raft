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
import Data.Time

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
-- RPC data types

type Term = Int

data AppendEntriesReq a = AppendEntriesReq
  { areqRequestId    :: Int
  , areqTerm         :: Term
  , areqLeaderId     :: NodeId
  , areqPrevLogIndex :: Int
  , areqPrevLogTerm  :: Term
  , areqEntries      :: [a]
  , areqLeaderCommit :: Int
  } deriving (Show, Generic, Typeable)

instance Binary a => Binary (AppendEntriesReq a)

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
  , vreqLastLogTerm  :: Int
  } deriving (Show, Generic, Typeable)

instance Binary RequestVoteReq

data RequestVoteResp = RequestVoteResp
  { vrespRequestId   :: Int
  , vrespTerm        :: Term
  , vrespNodeId      :: NodeId
  , vrespVoteGranted :: Bool
  } deriving (Show, Generic, Typeable)

instance Binary RequestVoteResp

data RaftRequest a
  = AppendEntriesRPC (AppendEntriesReq a)
  | RequestVoteRPC RequestVoteReq

----------------------------------------------------------------------
-- State

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
  { pendingRpc      :: M.Map Int (UTCTime, RaftRequest a)
  , rpcCounter      :: Int
  , currentTerm     :: Term
  , currentLog      :: [((Term, Int), a)]
  , commitIndex     :: Int
  , lastApplied     :: Int
  , nextIndex       :: M.Map NodeId Int
  , matchIndex      :: M.Map NodeId Int
  }

initRaftState :: RaftState a
initRaftState = RaftState
  { pendingRpc   = M.empty
  , rpcCounter   = 0
  , currentTerm  = 0
  , currentLog   = []
  , commitIndex  = 0
  , lastApplied  = 0
  , nextIndex    = M.empty
  , matchIndex   = M.empty

  }

getLastLogItem :: RaftState a -> (Term, Int)
getLastLogItem = foldr (\(coords, _) _-> coords) (0, 0) . currentLog

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

----------------------------------------------------------------------
-- Behaviors

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
            (_, _) <- checkCommit commit st
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
  pokes st $ \s -> s { currentTerm = succ (currentTerm s) }
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

        , match $ \(Reminder ()) -> do
            return (Left Candidate)

        , matchUnknown (say "Unknown message" >> return (Right n))
        ]
      case vote of
        Left newRole -> return newRole
        Right rem -> collectVotes rem



follower :: (Serializable a) => RaftConfig -> TVar (RaftState a) -> Maybe NodeId -> Process ()
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
    ]
  case res of
    Nothing -> candidate cfg st
    Just newLeader -> follower cfg st newLeader

----------------------------------------------------------------------
-- RPC handlers

-- * Election

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
checkVote r@RequestVoteResp{..} st = do
  say $ "Checking vote: " ++ show r
  pokes st $ \s -> s { currentTerm = max vrespTerm (currentTerm s) }
  req <- lookupRpc st vrespRequestId
  case req of
    Nothing ->
      return (vrespNodeId, False)
    Just _  -> do
      return (vrespNodeId, vrespVoteGranted)

-- * Log replication

sendHeartbeat :: (Serializable a) => TVar (RaftState a) -> NodeId -> Process ()
sendHeartbeat st node = do
  say $ "Heartbeat to " ++ show node
  term <- peeks st currentTerm
  leaderId <- getSelfNode
  leaderCommit <- peeks st commitIndex
  log <- peeks st currentLog

  request <- issueRpc st AppendEntriesRPC $ \reqid ->
    AppendEntriesReq
      { areqRequestId    = reqid
      , areqTerm         = term
      , areqLeaderId     = leaderId
      , areqPrevLogIndex = 0
      , areqPrevLogTerm  = 0
      , areqEntries      = map snd $ take 0 log
      , areqLeaderCommit = leaderCommit
      }

  sendNode node request

appendEntries :: AppendEntriesReq a -> TVar (RaftState a) -> Maybe NodeId -> Process (NodeId, Bool)
appendEntries AppendEntriesReq{..} st votedFor = do
  say $ "Heartbeat from leader " ++ show areqLeaderId

  success <- liftIO $ atomically $ do
    s <- readTVar st
    let legitimate =
          areqTerm > (currentTerm s) ||
          areqTerm == (currentTerm s) && votedFor == Just areqLeaderId

        (lastLogTerm, lastLogIndex) = getLastLogItem s

        canAppend =
          areqPrevLogTerm == lastLogTerm  &&
          areqPrevLogIndex == lastLogIndex

        newEntries = zip (zip (repeat areqTerm) [lastLogIndex..]) areqEntries

    if legitimate && canAppend
      then do
        modifyTVar st $ \s ->
          s { currentTerm = max areqTerm (currentTerm s)
            , currentLog = reverse newEntries ++ currentLog s
            }
        return True
      else
        return False

  self <- getSelfNode
  newTerm <- peeks st currentTerm
  sendNode areqLeaderId AppendEntriesResp
    { arespRequestId = areqRequestId
    , arespTerm = newTerm
    , arespNodeId = self
    , arespSuccess = success
    }

  return (areqLeaderId, success)

checkCommit :: AppendEntriesResp -> TVar (RaftState a) -> Process (NodeId, Bool)
checkCommit AppendEntriesResp{..} st = do

  liftIO $ atomically $ do
    let modifyNextIndex idx =
          if arespSuccess
          then idx
          else pred idx

        modifyMatchIndex _idx =
          if arespSuccess
          then undefined
          else undefined

    modifyTVar st $ \s ->
      s { nextIndex  = M.adjust modifyNextIndex arespNodeId (nextIndex s)
        , matchIndex = M.adjust modifyMatchIndex arespNodeId (matchIndex s)
        , currentTerm = max arespTerm (currentTerm s)
        }

  if arespSuccess
    then say "Successful append from follower"
    else say "Unsuccessful append from follower"

  return (arespNodeId, arespSuccess)

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
quorumCount me peers = 1 + (length (filter (/=me) peers) + 1) `div` 2

randomElectionTimeout :: Int -> Process Int
randomElectionTimeout base =
  (*) <$> pure (base `div` 1000)
      <*> liftIO (randomRIO (1000, 2000))
