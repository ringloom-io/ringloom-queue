const std = @import("std");

pub const config = @import("ringloom/config.zig");
pub const errors = @import("ringloom/errors.zig");
pub const header = @import("ringloom/header.zig");
pub const metadata = @import("ringloom/metadata.zig");
pub const index = @import("ringloom/index.zig");
pub const roll = @import("ringloom/roll.zig");
pub const codec = @import("ringloom/codec.zig");
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
