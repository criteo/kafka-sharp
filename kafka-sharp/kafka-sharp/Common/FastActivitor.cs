using System.Linq.Expressions;
using System.Reflection.Emit;

namespace System
{
    /// <summary>
    /// Dirty hack that allows using a fast implementation
    /// of the activator.
    /// </summary>
    static class Activator
    {
        public static T CreateInstance<T>() where T : new()
        {
#if DEBUG
            Console.WriteLine("Fast Activator was called");
#endif
            return ActivatorImpl<T>.Create();
        }

        private static class ActivatorImpl<T> where T : new()
        {
            public static readonly Func<T> Create =
                DynamicModuleLambdaCompiler.GenerateFactory<T>();
        }

        internal static class DynamicModuleLambdaCompiler
        {
            public static Func<T> GenerateFactory<T>() where T : new()
            {
                Expression<Func<T>> expr = () => new T();
                NewExpression newExpr = (NewExpression)expr.Body;

                var method = new DynamicMethod(
                    name: "lambda",
                    returnType: newExpr.Type,
                    parameterTypes: new Type[0],
                    m: typeof(DynamicModuleLambdaCompiler).Module,
                    skipVisibility: true);

                ILGenerator ilGen = method.GetILGenerator();
                // Constructor for value types could be null
                if (newExpr.Constructor != null)
                {
                    ilGen.Emit(OpCodes.Newobj, newExpr.Constructor);
                }
                else
                {
                    LocalBuilder temp = ilGen.DeclareLocal(newExpr.Type);
                    ilGen.Emit(OpCodes.Ldloca, temp);
                    ilGen.Emit(OpCodes.Initobj, newExpr.Type);
                    ilGen.Emit(OpCodes.Ldloc, temp);
                }

                ilGen.Emit(OpCodes.Ret);

                return (Func<T>)method.CreateDelegate(typeof(Func<T>));
            }
        }

    }
}
