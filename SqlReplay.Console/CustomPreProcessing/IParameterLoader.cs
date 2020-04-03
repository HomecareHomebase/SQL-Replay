namespace SqlReplay.Console.CustomPreProcessing
{
    interface IParameterLoader
    {
        void LoadParameters(Rpc rpc);
    }
}
