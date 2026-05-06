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
    MetadataMagicMismatch,
    MetadataVersionMismatch,
    QueueFileMagicMismatch,
    QueueFileVersionMismatch,
    CreateNotPermitted,
    CreateRequiresRollScheme,
    CreateRequiresEmptyDir,

    RollFormatNotRecognized,
    RollFormatMissing,
    RollLengthMissing,
    RollEpochMissing,
    UnrecognizedFormatToken,
    UnterminatedQuote,

    MetadataParseFailed,
    MetadataFieldsMissing,
    MetadataReopenFailed,

    MessageTooLarge,
    WriteConflict,

    IndexSlotOutOfBounds,
    IndexRegionCorrupted,

    PlatformCapabilityUnavailable,
    PrefetchFailed,
    CleanerFailed,

    PrerollCreateFailed,
    PrerollPreallocateFailed,

    OutOfMemory,
};
