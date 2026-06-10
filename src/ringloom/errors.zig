// SPDX-License-Identifier: Apache-2.0

pub const RingloomError = error{
    DirStatFailed,
    NotADirectory,
    OpenFailed,
    StatFailed,
    MmapFailed,
    MunmapFailed,
    MadviseFailed,
    SeekFailed,
    WriteFailed,
    RenameFailed,
    PreallocateFailed,

    QueueIsNull,
    QueueNotFound,
    MetadataMagicMismatch,
    MetadataVersionMismatch,
    InvalidMagic,
    InvalidVersion,
    MetadataCorrupt,
    QueueFileMagicMismatch,
    QueueFileVersionMismatch,
    InvalidQueueFileHeader,
    CreateNotPermitted,
    CreateRequiresRollScheme,
    CreateRequiresEmptyDir,

    RollFormatNotRecognized,
    UnknownRollScheme,
    InvalidRollConfig,
    RollConfigMismatch,
    RollFormatMissing,
    RollLengthMissing,
    RollEpochMissing,
    UnrecognizedFormatToken,
    UnterminatedQuote,

    MetadataParseFailed,
    MetadataFieldsMissing,
    MetadataReopenFailed,

    MessageTooLarge,
    EmptyPayload,
    WriteConflict,

    IndexSlotOutOfBounds,
    IndexRegionCorrupted,

    DuplicateIndex,
    IndexGap,

    PlatformCapabilityUnavailable,
    PrefetchFailed,
    CleanerFailed,

    PrerollCreateFailed,
    PrerollPreallocateFailed,
    PrerollFailed,

    AppenderAlreadyOpen,
    AppenderLeaseLost,

    OutOfMemory,
};
