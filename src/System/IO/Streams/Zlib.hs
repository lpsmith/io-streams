-- | Interface to @zlib@ and @gzip@ compression for 'Bytestring's and 'Builder's

{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module System.IO.Streams.Zlib
 ( -- * ByteString decompression
   gunzip
 , gunzipOne
 , decompress
   -- * ByteString compression
 , gzip
 , compress
   -- * Builder compression
 , gzipBuilder
 , compressBuilder
   -- * Compression level
 , CompressionLevel(..)
 , defaultCompressionLevel
 ) where

------------------------------------------------------------------------------
import           Control.Exception                (throwIO)
import           Control.Monad                    (join)
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString                  as S
import           Data.IORef                       (newIORef, readIORef, writeIORef)
import           Data.Word                        (Word8)
import           Prelude                          hiding (read)
------------------------------------------------------------------------------
import           Codec.Compression.Zlib.Internal  (Format, DecompressParams, DecompressStream(..), decompressIO, zlibFormat, gzipFormat, defaultDecompressParams)
import           Codec.Zlib                       (Deflate, WindowBits (..), feedDeflate, finishDeflate, flushDeflate, initDeflate)
import           Data.ByteString.Builder          (Builder, byteString)
import           Data.ByteString.Builder.Extra    (defaultChunkSize, flush)
import           Data.ByteString.Builder.Internal (newBuffer)
------------------------------------------------------------------------------
import           System.IO.Streams.Builder        (unsafeBuilderStream)
import           System.IO.Streams.Internal       (InputStream, OutputStream, makeInputStream, makeOutputStream, read, write, unRead)


------------------------------------------------------------------------------
gzipBits :: WindowBits
gzipBits = WindowBits 31


------------------------------------------------------------------------------
compressBits :: WindowBits
compressBits = WindowBits 15


------------------------------------------------------------------------------
-- | Decompress an 'InputStream' of strict 'ByteString's from the @gzip@ format
gunzip :: InputStream ByteString -> IO (InputStream ByteString)
gunzip = inflateMulti 0x1F 0x8B gzipFormat defaultDecompressParams


------------------------------------------------------------------------------
-- | Decompress a single gzip stream from a an 'InputStream'.
gunzipOne :: InputStream ByteString -> IO (InputStream ByteString)
gunzipOne = inflateOne gzipFormat defaultDecompressParams


------------------------------------------------------------------------------
-- | Decompress an 'InputStream' of strict 'ByteString's from the @zlib@ format
decompress :: InputStream ByteString -> IO (InputStream ByteString)
decompress = inflateOne zlibFormat defaultDecompressParams


------------------------------------------------------------------------------
-- | Decompress a single compressed stream
inflateOne :: Format -> DecompressParams
           -> InputStream ByteString -> IO (InputStream ByteString)
inflateOne fmt params input = do
    ref <- newIORef (return $ decompressIO fmt params)
    makeInputStream $ stream ref
  where
    stream ref = join (readIORef ref) >>= go
      where
        go st =
            case st of
              DecompressInputRequired feed -> do
                  compressed <- readNonEmpty input
                  feed (maybe S.empty id compressed) >>= go
              DecompressOutputAvailable out next -> do
                  writeIORef ref next
                  return (Just out)
              DecompressStreamEnd crumb -> do
                  unRead crumb input
                  return Nothing
              DecompressStreamError err -> do
                  throwIO err


------------------------------------------------------------------------------
-- | Decompress one or more compressed streams
inflateMulti :: Word8 -> Word8 -> Format -> DecompressParams
             -> InputStream ByteString -> IO (InputStream ByteString)
inflateMulti magic0 magic1 fmt params input = do
    ref <- newIORef undefined
    writeIORef ref (initStream ref)
    makeInputStream $ join (readIORef ref)
  where
    initStream ref = init
      where
        init  = go (decompressIO fmt params)
        go st =
            case st of
              DecompressInputRequired feed -> do
                  compressed <- readNonEmpty input
                  feed (maybe S.empty id compressed) >>= go
              DecompressOutputAvailable out next -> do
                  writeIORef ref (next >>= go)
                  return (Just out)
              DecompressStreamEnd crumb -> do
                  unRead crumb input
                  continue <- checkMagicBytes magic0 magic1 input
                  if continue
                  then init
                  else return Nothing
              DecompressStreamError err -> do
                  throwIO err

readNonEmpty :: InputStream ByteString -> IO (Maybe ByteString)
readNonEmpty input = do
    ma <- read input
    case ma of
      Just a | S.null a -> readNonEmpty input
      _ -> return ma

checkMagicBytes :: Word8 -> Word8 -> InputStream ByteString -> IO Bool
checkMagicBytes magic0 magic1 input = do
    ma <- readNonEmpty input
    case ma of
      Nothing -> return False
      Just a
        | S.length a > 1 ->
          do
             unRead a input
             return (S.index a 0 == magic0 && S.index a 1 == magic1)
        | otherwise {- S.length a == 1 -} ->
          do
             if S.index a 0 /= magic0
             then do
               unRead a input
               return False
             else do
               mb <- readNonEmpty input
               case mb of
                 Nothing -> do
                    unRead a input
                    return False
                 Just b  -> do
                    unRead b input
                    unRead a input
                    return (S.index b 0 == magic1)


------------------------------------------------------------------------------
deflateBuilder :: OutputStream Builder
               -> Deflate
               -> IO (OutputStream Builder)
deflateBuilder stream state = do
    zippedStr <- makeOutputStream bytestringStream >>=
                 \x -> deflate x state

    -- we can use unsafeBuilderStream here because zlib is going to consume the
    -- stream
    unsafeBuilderStream (newBuffer defaultChunkSize) zippedStr

  where
    bytestringStream x = write (fmap cvt x) stream

    cvt s | S.null s  = flush
          | otherwise = byteString s


------------------------------------------------------------------------------
-- | Convert an 'OutputStream' that consumes compressed 'Builder's into an
-- 'OutputStream' that consumes uncompressed 'Builder's in the @gzip@ format
gzipBuilder :: CompressionLevel
            -> OutputStream Builder
            -> IO (OutputStream Builder)
gzipBuilder level output =
    initDeflate (clamp level) gzipBits >>= deflateBuilder output


------------------------------------------------------------------------------
-- | Convert an 'OutputStream' that consumes compressed 'Builder's into an
-- 'OutputStream' that consumes uncompressed 'Builder's in the @zlib@ format
compressBuilder :: CompressionLevel
                -> OutputStream Builder
                -> IO (OutputStream Builder)
compressBuilder level output =
    initDeflate (clamp level) compressBits >>= deflateBuilder output


------------------------------------------------------------------------------
deflate :: OutputStream ByteString
        -> Deflate
        -> IO (OutputStream ByteString)
deflate output state = makeOutputStream stream
  where
    stream Nothing = popAll (finishDeflate state) >> write Nothing output

    stream (Just s) = do
        -- Empty string means flush
        if S.null s
          then do
              popAll (flushDeflate state)
              write (Just S.empty) output

          else feedDeflate state s >>= popAll


    popAll popper = go
      where
        go = popper >>= maybe (return $! ()) (\s -> write (Just s) output >> go)


------------------------------------------------------------------------------
-- | Parameter that defines the tradeoff between speed and compression ratio
newtype CompressionLevel = CompressionLevel Int
  deriving (Read, Eq, Show, Num)


------------------------------------------------------------------------------
-- | A compression level that balances speed with compression ratio
defaultCompressionLevel :: CompressionLevel
defaultCompressionLevel = CompressionLevel 5


------------------------------------------------------------------------------
clamp :: CompressionLevel -> Int
clamp (CompressionLevel x) = min 9 (max x 0)


------------------------------------------------------------------------------
-- | Convert an 'OutputStream' that consumes compressed 'ByteString's into an
-- 'OutputStream' that consumes uncompressed 'ByteString's in the @gzip@ format
gzip :: CompressionLevel
     -> OutputStream ByteString
     -> IO (OutputStream ByteString)
gzip level output = initDeflate (clamp level) gzipBits >>= deflate output


------------------------------------------------------------------------------
-- | Convert an 'OutputStream' that consumes compressed 'ByteString's into an
-- 'OutputStream' that consumes uncompressed 'ByteString's in the @zlib@ format
compress :: CompressionLevel
         -> OutputStream ByteString
         -> IO (OutputStream ByteString)
compress level output = initDeflate (clamp level) compressBits >>=
                        deflate output
