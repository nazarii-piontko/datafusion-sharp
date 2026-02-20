using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct CollectedRecordBatches
{
    public Apache.Arrow.C.CArrowSchema* Schema;
    public int NumBatches;
    public Apache.Arrow.C.CArrowArray* Batches;
}