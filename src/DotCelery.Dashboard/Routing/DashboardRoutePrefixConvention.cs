using DotCelery.Dashboard.Controllers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ApplicationModels;

namespace DotCelery.Dashboard.Routing;

internal sealed class DashboardRoutePrefixConvention : IApplicationModelConvention
{
    private readonly AttributeRouteModel _routePrefix;

    public DashboardRoutePrefixConvention(string pathPrefix)
    {
        var normalizedPrefix = NormalizePrefix(pathPrefix);
        _routePrefix = new AttributeRouteModel(new RouteAttribute(normalizedPrefix));
    }

    public void Apply(ApplicationModel application)
    {
        foreach (var controller in application.Controllers)
        {
            if (controller.ControllerType.AsType() != typeof(DashboardApiController))
            {
                continue;
            }

            foreach (var selector in controller.Selectors)
            {
                selector.AttributeRouteModel =
                    selector.AttributeRouteModel is null
                        ? _routePrefix
                        : AttributeRouteModel.CombineAttributeRouteModel(
                            _routePrefix,
                            selector.AttributeRouteModel
                        );
            }
        }
    }

    private static string NormalizePrefix(string pathPrefix)
    {
        var prefix = pathPrefix.Trim();
        if (string.IsNullOrEmpty(prefix) || prefix == "/")
        {
            return "";
        }

        if (prefix[0] == '/')
        {
            prefix = prefix[1..];
        }

        return prefix.TrimEnd('/');
    }
}
