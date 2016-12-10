{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE DeriveGeneric       #-}

module Control.Distributed.Process.Reminder
  ( Reminder(..)
  , remindAfter
  ) where

import Control.Concurrent (threadDelay)
import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Data.Binary (Binary)
import Data.Typeable
import GHC.Generics

data Reminder a = Reminder a
  deriving (Show, Generic, Typeable)
instance Binary a => Binary (Reminder a)

-- | Remind current process after a timeout @micros@ in microseconds
-- with some value @payload@. It'll be reminded with a message of type
-- 'Reminder'.
remindAfter :: (Serializable a) => Int -> a -> Process ProcessId
remindAfter micros payload = do
  pid <- getSelfPid
  spawnLocal $ do
    link pid
    liftIO $ threadDelay micros
    send pid (Reminder payload)
