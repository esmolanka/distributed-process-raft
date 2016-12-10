{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Options.Applicative

import Control.Monad (forM_)
import Control.Concurrent (threadDelay)
import Control.Distributed.Process
import Control.Distributed.Process.Node
import Data.Hashable

import Network.Transport
import Network.Transport.TCP (createTransport, defaultTCPParameters)

import qualified Data.ByteString.Char8 as B8
import qualified Data.Set as S

-- Local module
import Control.Distributed.Process.Raft

data Config = Config
  { cfgPeersFile :: FilePath
  , cfgListen :: String
  , cfgHeartbeatRate :: Int -- millis
  , cfgElectionTimeout :: Int -- millis
  }

config :: Parser Config
config = Config
  <$>  strOption
        (long "peers" <>
         metavar "FILE" <>
         help "specify whitespace-separated list of peers")
  <*>  strOption
        (long "listen" <> short 'l' <>
         metavar "IP:PORT" <>
         help "specify current node's hostname:port to listen on")
  <*>  option auto
        (long "heartbeat" <> short 'b' <>
        metavar "MILLIS" <>
        help "set hearbeat in milliseconds" <>
        value 30
        )
  <*>  option auto
        (long "election-timeout" <> short 'e' <>
        metavar "MILLIS" <>
        help "set election timeout's lower boundary (higher boundary is 2x of lower boundary)" <>
        value 100
        )

makeNodeId :: String -> NodeId
makeNodeId = NodeId . EndPointAddress . B8.pack . (++ ":0")

readPeers :: FilePath -> IO [NodeId]
readPeers fn = map makeNodeId . words <$> readFile fn

main :: IO ()
main = do
  cfg <-
    execParser $ info
      (helper <*> config)
      (fullDesc <> progDesc "Raft testing node")

  let host = takeWhile (/= ':') (cfgListen cfg)
      port = drop 1 . dropWhile (/= ':') $ (cfgListen cfg)

  peerNodes <-
    S.toList .
      S.delete (makeNodeId (cfgListen cfg)) .
        S.fromList <$>
          readPeers (cfgPeersFile cfg)

  transport <-
    either (error . show) id <$>
      createTransport host port defaultTCPParameters

  node <- newLocalNode transport initRemoteTable

  let raftConfig =
        RaftConfig
          (cfgElectionTimeout cfg)
          (cfgHeartbeatRate cfg)
          peerNodes

  runProcess node $ do
    server <- startRaft raftConfig
    self <- getSelfNode
    forM_ [1..100] $ \msg ->
      let try = do
            result <- commit server (mkMessage self msg)
            liftIO (threadDelay 100000) -- 10 milliseconds
            case result of
              False -> say "Retrying commit..." >> try
              True -> return ()
      in try
    liftIO (threadDelay 3000000) -- 3 seconds
    say "Retrieving..."
    log <- maybe [] id <$> retrieve server
    liftIO (print $ hash log)
    liftIO (writeFile (cfgListen cfg ++ ".output") (unlines log))
    liftIO (threadDelay 100000) -- 0.1 second

mkMessage :: NodeId -> Int -> String
mkMessage nodeid counter =
  show nodeid ++ " says 'This is my message number " ++ show counter ++ "'."
