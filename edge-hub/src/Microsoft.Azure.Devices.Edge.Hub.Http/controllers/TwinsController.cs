
namespace Microsoft.Azure.Devices.Edge.Hub.Http.Controllers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Authorization.Infrastructure;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.AspNetCore.Mvc.Abstractions;
    using Microsoft.AspNetCore.Mvc.Authorization;
    using Microsoft.AspNetCore.Mvc.Filters;
    using Microsoft.AspNetCore.Mvc.Infrastructure;
    using Microsoft.AspNetCore.Mvc.Internal;
    using Microsoft.Azure.Devices.Edge.Hub.Core;
    using Microsoft.Azure.Devices.Edge.Hub.Core.Identity;
    using Microsoft.Azure.Devices.Edge.Util;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public class TwinsController : Controller
    {
        readonly Task<IEdgeHub> edgeHubGetter;
        readonly IValidator<MethodRequest> validator;
        IIdentity identity;
        bool showRoutes;

        readonly IActionDescriptorCollectionProvider provider;

        public TwinsController(Task<IEdgeHub> edgeHub, IValidator<MethodRequest> validator, IActionDescriptorCollectionProvider provider)
        {
            this.edgeHubGetter = Preconditions.CheckNotNull(edgeHub, nameof(edgeHub));
            this.validator = Preconditions.CheckNotNull(validator, nameof(validator));

            this.provider = provider;
        }

        public string GetRoutes()
        {
            IEnumerable<ActionDescriptor> openRoutes = this.provider.ActionDescriptors.Items
                .Where(
                    x => x.FilterDescriptors.All(f => f.Filter.GetType() != typeof(AuthorizeFilter)) ||
                        x.FilterDescriptors.Any(f => f.Filter.GetType() == typeof(AllowAnonymousFilter)));

            var openRoutesDisplay = openRoutes
                .Select(x => $"{x?.ActionConstraints?.OfType<HttpMethodActionConstraint>().FirstOrDefault()?.HttpMethods.First()} -> {x.AttributeRouteInfo.Template}");

            var roleGroupedRoutesDisplay = this.provider.ActionDescriptors.Items
                .Except(openRoutes)
                .GroupBy(r => this.GetAuthorizationRole(r))
                .SelectMany(
                    g =>
                        g.Select(x => $"[{g.Key}] {x?.ActionConstraints?.OfType<HttpMethodActionConstraint>().FirstOrDefault()?.HttpMethods.First()} -> {x.AttributeRouteInfo.Template}")
                ).ToArray();
            return string.Join(Environment.NewLine, openRoutesDisplay
                    .Concat(new[] { "-------- SECURED ROUTES --------" })
                    .Concat(roleGroupedRoutesDisplay));
        }

        public string GetAuthorizationRole(ActionDescriptor action)
        {
            var allowedRoles = ((RolesAuthorizationRequirement) action.FilterDescriptors.Where(x => x.Filter.GetType() == typeof(AuthorizeFilter))
                .SelectMany(x => ((AuthorizeFilter) x.Filter).Policy.Requirements)
                .FirstOrDefault(x => x.GetType() == typeof(RolesAuthorizationRequirement)))?.AllowedRoles;

            if (allowedRoles == null)
            {
                return "Authenticated";
            }

            return string.Join(", ", allowedRoles);
        }

        public override void OnActionExecuting(ActionExecutingContext context)
        {
            if (!this.showRoutes)
            {
                this.showRoutes = true;
                Console.WriteLine("Output all defined routes:");
                Console.WriteLine(this.GetRoutes());
            }

            if (context.HttpContext.Items.TryGetValue(HttpConstants.IdentityKey, out object contextIdentity))
            {
                this.identity = contextIdentity as IIdentity;
            }
            base.OnActionExecuting(context);
        }

        [HttpPost]
        [Route("twins/{deviceId}/methods")]
        public Task<IActionResult> InvokeDeviceMethodAsync([FromRoute] string deviceId, [FromBody] MethodRequest methodRequest)
        {
            deviceId = WebUtility.UrlDecode(Preconditions.CheckNonWhiteSpace(deviceId, nameof(deviceId)));
            this.validator.Validate(methodRequest);

            var directMethodRequest = new DirectMethodRequest(deviceId, methodRequest.MethodName, methodRequest.PayloadBytes, methodRequest.ResponseTimeout, methodRequest.ConnectTimeout);
            return this.InvokeMethodAsync(directMethodRequest);
        }

        [HttpPost]
        [Route("twins/{deviceId}/modules/{moduleId}/methods")]
        public Task<IActionResult> InvokeModuleMethodAsync([FromRoute] string deviceId, [FromRoute] string moduleId, [FromBody] MethodRequest methodRequest)
        {
            deviceId = WebUtility.UrlDecode(Preconditions.CheckNonWhiteSpace(deviceId, nameof(deviceId)));
            moduleId = WebUtility.UrlDecode(Preconditions.CheckNonWhiteSpace(moduleId, nameof(moduleId)));
            this.validator.Validate(methodRequest);

            var directMethodRequest = new DirectMethodRequest($"{deviceId}/{moduleId}", methodRequest.MethodName, methodRequest.PayloadBytes, methodRequest.ResponseTimeout, methodRequest.ConnectTimeout);
            return this.InvokeMethodAsync(directMethodRequest);
        }

        async Task<IActionResult> InvokeMethodAsync(DirectMethodRequest directMethodRequest)
        {
            Events.ReceivedMethodCall(directMethodRequest, this.identity);
            IEdgeHub edgeHub = await this.edgeHubGetter;
            DirectMethodResponse directMethodResponse = await edgeHub.InvokeMethodAsync(this.identity.Id, directMethodRequest);
            Events.ReceivedMethodCallResponse(directMethodRequest, this.identity);

            MethodResult methodResult = GetMethodResult(directMethodResponse);
            HttpResponse response = this.Request?.HttpContext?.Response;
            if (response != null)
            {
                response.ContentLength = GetContentLength(methodResult);
            }
            return this.StatusCode((int)directMethodResponse.HttpStatusCode, methodResult);
        }

        static int GetContentLength(MethodResult methodResult)
        {
            string json = JsonConvert.SerializeObject(methodResult);
            return json.Length;
        }

        internal static MethodResult GetMethodResult(DirectMethodResponse directMethodResponse) =>
            directMethodResponse.Exception.Map(e => new MethodErrorResult(directMethodResponse.Status, null, e.Message, string.Empty) as MethodResult)
                .GetOrElse(() => new MethodResult(directMethodResponse.Status, GetRawJson(directMethodResponse.Data)));

        internal static JRaw GetRawJson(byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0)
            {
                return null;
            }

            string json = Encoding.UTF8.GetString(bytes);
            return new JRaw(json);
        }

        static class Events
        {
            static readonly ILogger Log = Logger.Factory.CreateLogger<TwinsController>();
            const int IdStart = HttpEventIds.TwinsController;

            enum EventIds
            {
                ReceivedMethodCall = IdStart,
                ReceivedMethodResponse
            }

            public static void ReceivedMethodCall(DirectMethodRequest methodRequest, IIdentity identity)
            {
                Log.LogDebug((int)EventIds.ReceivedMethodCall, $"Received call to invoke method {methodRequest.Name} on device or module {methodRequest.Id} from module {identity.Id}");
            }

            public static void ReceivedMethodCallResponse(DirectMethodRequest methodRequest, IIdentity identity)
            {
                Log.LogDebug((int)EventIds.ReceivedMethodResponse, $"Received response from call to method {methodRequest.Name} from device or module {methodRequest.Id}. Method invoked by module {identity.Id}");
            }
        }
    }
}
