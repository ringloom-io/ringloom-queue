const std = @import("std");

pub const config = @import("ringloom/config.zig");
pub const errors = @import("ringloom/errors.zig");
pub const header = @import("ringloom/header.zig");
pub const metadata = @import("ringloom/metadata.zig");
pub const index = @import("ringloom/index.zig");
pub const roll = @import("ringloom/roll.zig");
pub const codec = @import("ringloom/codec.zig");
pub const atomic_ops = @import("ringloom/atomic_ops.zig");
pub const mmap_ops = @import("ringloom/mmap_ops.zig");
pub const window = @import("ringloom/window.zig");
pub const file_ops = @import("ringloom/file_ops.zig");
pub const platform = @import("ringloom/platform.zig");
pub const prefetcher = @import("ringloom/prefetcher.zig");
pub const cleaner = @import("ringloom/cleaner.zig");
pub const appender = @import("ringloom/appender.zig");
pub const tailer = @import("ringloom/tailer.zig");
pub const queue = @import("ringloom/queue.zig");

pub const Header = header.Header;
pub const SharedMetadata = metadata.SharedMetadata;
pub const QueueFileHeader = metadata.QueueFileHeader;
pub const Index = index.Index;
pub const IndexRegion = index.IndexRegion;
pub const RollScheme = roll.RollScheme;
pub const Queue = queue.Queue;
pub const Tailer = tailer.Tailer;
pub const TailerState = tailer.TailerState;
pub const ParseBlockState = tailer.ParseBlockState;
pub const RawCollected = tailer.RawCollected;
pub const Collected = tailer.Collected;
pub const Appender = appender.Appender;
pub const Platform = platform.Platform;
pub const StepResult = platform.StepResult;
pub const Prefetcher = prefetcher.Prefetcher;
pub const Cleaner = cleaner.Cleaner;
pub const RingloomError = errors.RingloomError;

pub const cmpxchg32 = atomic_ops.cmpxchg32;
pub const atomicLoad32 = atomic_ops.atomicLoad32;
pub const atomicStore32 = atomic_ops.atomicStore32;
pub const atomicFetchAdd32 = atomic_ops.atomicFetchAdd32;
pub const atomicLoad64 = atomic_ops.atomicLoad64;
pub const atomicStore64 = atomic_ops.atomicStore64;
pub const atomicFetchAdd64 = atomic_ops.atomicFetchAdd64;
pub const casBackoff = atomic_ops.casBackoff;

pub const Protection = mmap_ops.Protection;
pub const MapFlags = mmap_ops.MapFlags;
pub const MetadataMap = mmap_ops.MetadataMap;
pub const mapFile = mmap_ops.mapFile;
pub const mapFileWithFallback = mmap_ops.mapFileWithFallback;
pub const unmapFile = mmap_ops.unmapFile;
pub const remapFile = mmap_ops.remapFile;
pub const adviseSequential = mmap_ops.adviseSequential;
pub const adviseWillNeed = mmap_ops.adviseWillNeed;
pub const adviseDontNeed = mmap_ops.adviseDontNeed;
pub const touchWritablePages = mmap_ops.touchWritablePages;
pub const touchReadablePages = mmap_ops.touchReadablePages;
pub const mapSharedMetadata = mmap_ops.mapSharedMetadata;

pub const WindowParams = window.WindowParams;
pub const MmapWindow = window.MmapWindow;
pub const PremappedWindow = window.PremappedWindow;
pub const computeWindow = window.computeWindow;
pub const needsRemap = window.needsRemap;
pub const shouldPremap = window.shouldPremap;

pub const alignUp = file_ops.alignUp;
pub const needsExtension = file_ops.needsExtension;
pub const extendFile = file_ops.extendFile;
pub const extendFilePermissive = file_ops.extendFilePermissive;
pub const ensureFileSize = file_ops.ensureFileSize;
pub const ensureFileSizePermissive = file_ops.ensureFileSizePermissive;

pub const Codec = codec.Codec;
pub const CodecError = codec.CodecError;
pub const Dispatcher = codec.Dispatcher;
pub const DispatchAction = codec.DispatchAction;
pub const RawCodec = codec.RawCodec;
pub const TextCodec = codec.TextCodec;
pub const DefaultRawCodec = codec.DefaultRawCodec;
pub const StructCodec = codec.StructCodec;
pub const RuntimeCodec = codec.RuntimeCodec;

test {
    std.testing.refAllDecls(@This());
}
