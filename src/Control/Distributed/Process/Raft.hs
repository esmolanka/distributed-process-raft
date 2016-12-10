{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Control.Distributed.Process.Raft
  ( startRaft
  , commit
  , retrieve
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

import Data.Binary (Binary)
import Data.List
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Time
import Data.Typeable

import GHC.Generics

----------------------------------------------------------------------
-- Interface

data Raft a = Raft ProcessId (TVar Int)

startRaft :: forall a. (Show a, Serializable a) => RaftConfig -> Process (Raft a)
startRaft cfg = do
  st <- liftIO $ newTVarIO (initRaftState :: RaftState a)
  reqCounter <- liftIO $ newTVarIO 0

  pid <- spawnLocal $ do
    self <- getSelfPid
    register raftServerName self
    follower cfg st Nothing

  return $ Raft pid reqCounter

commit :: forall a. (Serializable a) => Raft a -> a -> Process Bool
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

retrieve :: forall a. (Serializable a) => Raft a -> Process (Maybe [a])
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

----------------------------------------------------------------------
-- RPC data types

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

data RaftConfig = RaftConfig
  { electionTimeout :: Int -- milliseconds
  , heartbeatRate   :: Int -- milliseconds
  , peers           :: [NodeId]
  }

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

----------------------------------------------------------------------
-- Behaviors

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

-- * Log replication

sendHeartbeat :: (Show a, Serializable a) => TVar (RaftState a) -> NodeId -> Process ()
sendHeartbeat st node = do
  say $ "Heartbeat to " ++ show node
  leaderId <- getSelfNode
  RaftState{..} <- peeks st id

  let currentNextIndex = M.findWithDefault 0 node nextIndex

  let (entriesToSend, restEntries) = partition (\((_, n), _) -> n >= currentNextIndex) currentLog
      (prevLogTerm, prevLogIndex)  = foldr (\(idx, _) _ -> idx) (Term 0, 0) restEntries

  -- say $ "Presumed state on " ++ show node ++ ": curNextIndex=" ++ show currentNextIndex ++"\n" ++ show restEntries

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

-- * Client

clientCommit :: forall a. (Serializable a) => ClientCommitReq a -> TVar (RaftState a) -> Maybe NodeId -> Process ()
clientCommit req@ClientCommitReq{..} st leaderId = do
  iamleader <- (== leaderId) . Just <$> getSelfNode
  case creqPayload of
    Nothing -> respondWithLog
    Just el
      | iamleader -> appendToClientQueue el respondWithLog
      | otherwise -> sendToLeader

  where
    appendToClientQueue :: a -> Process () -> Process ()
    appendToClientQueue element action = do
      liftIO $ atomically $ do
        newLogIndex <- succ . snd . getLastLogItem <$> readTVar st
        modifyTVar' st $ \s ->
          s { currentLog = ((currentTerm s, newLogIndex), element) : currentLog s
            , clientQueue = M.insertWith (++) newLogIndex [action] (clientQueue s)
            }

    respondWithLog = do
      s <- peeks st id
      send creqClientPid ClientCommitResp
        { crespRequestId = creqRequestId
        , crespLog = if creqRequestLog
                     then Just . reverse . map snd .
                          dropWhile (\((_, n), _) -> n > commitIndex s) $
                          currentLog s
                     else Nothing
        , crespCommitIndex = commitIndex s
        , crespSuccess     = True
        }

    sendToLeader = do
      case leaderId of
        Nothing ->
          send creqClientPid ClientCommitResp
            { crespRequestId = creqRequestId
            , crespLog = (Nothing :: Maybe [a])
            , crespCommitIndex = 0
            , crespSuccess = False
            }
        Just node -> sendNode node req




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
