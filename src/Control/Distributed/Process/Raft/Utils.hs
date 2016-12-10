module Control.Distributed.Process.Raft.Utils where

import System.Random
import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Raft.Types

sendNode :: (Serializable a) => NodeId -> a -> Process ()
sendNode nid a = nsendRemote nid raftServerName a

quorumCount :: NodeId -> [NodeId] -> Int
quorumCount me peers = 1 + (length (filter (/=me) peers) + 1) `div` 2

randomElectionTimeout :: Int -> Process Int
randomElectionTimeout base =
  (*) <$> pure (base `div` 1000)
      <*> liftIO (randomRIO (1000, 2000))
