using System.Collections.Generic;
using System.Linq;
using SqlReplay.Console.CustomPreProcessing.ParameterSignatures;

namespace SqlReplay.Console.CustomPreProcessing
{
    internal class GdasParameterLoader : IParameterLoader
    {
        private int currentAgentIndex = 0;

        public void LoadParameters(Rpc rpc)
        {
            var agentIdParam = Gdas.AgentId;
            agentIdParam.Value = getAgentID();

            rpc.Parameters.Add(agentIdParam);
            rpc.Parameters.Add(Gdas.LastRenewTime);
        }

        private int getAgentID()
        {
            int returnId = activeAgentIds[currentAgentIndex];
            currentAgentIndex = currentAgentIndex + 1 >= activeAgentIds.Count() ? 0 : currentAgentIndex + 1;

            return returnId;
        }

        // Selection of active LHC users during development
        private readonly int[] activeAgentIds = { 628174, 628173, 628172, 628171, 628170, 628169, 628167, 628166,
                                                  628165, 628164, 628162, 588968, 588804, 588788, 588774, 588767,
                                                  588764, 588754, 588740, 588729, 588724, 588718, 561529, 561528,
                                                  561520, 561516, 561513, 561506, 561503, 561491, 561487, 561072,
                                                  509531, 509451, 509432, 509144, 509134, 509124, 509098, 508971,
                                                  508939, 508759, 164357, 164046, 163900, 163882, 163881, 163876,
                                                  163873, 163858, 163557, 163409, 111287, 111280, 111274, 111268,
                                                  111262, 111254, 111249, 111217, 111212, 111205 };
    
    }
}
