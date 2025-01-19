using System.Threading.Tasks;

namespace ETag;

class Program
{
    static async Task Main(string[] args)
    {
        await OccSimulation.RunSimulation();
    }
}
