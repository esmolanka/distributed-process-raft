{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Control.Distributed.Process.Raft.ClientRpc
  ( clientCommit
  ) where

import Control.Monad.Reader
import Control.Concurrent.STM

import Control.Distributed.Process
import Control.Distributed.Process.Serializable

import qualified Data.Map.Strict as M

import Control.Distributed.Process.Raft.Types
import Control.Distributed.Process.Raft.Utils

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
