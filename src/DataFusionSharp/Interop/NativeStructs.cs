using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct NativeDataFrameCollectedData
{
    public Apache.Arrow.C.CArrowSchema* Schema;
    public int NumBatches;
    public Apache.Arrow.C.CArrowArray* Batches;
}

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct NativeDataFrameExecutedStreamData
{
    public IntPtr StreamHandle;
    public Apache.Arrow.C.CArrowSchema* Schema;
}