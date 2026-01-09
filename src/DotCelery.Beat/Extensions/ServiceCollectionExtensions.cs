using DotCelery.Core.Abstractions;
using DotCelery.Core.Canvas;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Beat.Extensions;

/// <summary>
/// Extension methods for configuring the Beat scheduler.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Beat scheduler to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureSchedule">Action to configure the schedule.</param>
    /// <param name="configureOptions">Optional action to configure options.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddBeatScheduler(
        this IServiceCollection services,
        Action<Schedule> configureSchedule,
        Action<BeatOptions>? configureOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(configureSchedule);

        var schedule = new Schedule();
        configureSchedule(schedule);

        services.AddSingleton(schedule);

        if (configureOptions is not null)
        {
            services.Configure(configureOptions);
        }

        services.AddHostedService<BeatSchedulerService>();

        return services;
    }

    /// <summary>
    /// Adds a cron-based schedule entry.
    /// </summary>
    /// <param name="schedule">The schedule.</param>
    /// <param name="name">Entry name.</param>
    /// <param name="cron">Cron expression.</param>
    /// <param name="taskName">Task name.</param>
    /// <param name="args">Optional task arguments.</param>
    /// <param name="options">Optional schedule options.</param>
    /// <returns>The schedule for chaining.</returns>
    public static Schedule AddCron(
        this Schedule schedule,
        string name,
        string cron,
        string taskName,
        byte[]? args = null,
        ScheduleOptions? options = null
    )
    {
        schedule.Add(
            new ScheduleEntry
            {
                Name = name,
                Cron = cron,
                Task = new Signature { TaskName = taskName, Args = args },
                Options = options,
            }
        );

        return schedule;
    }

    /// <summary>
    /// Adds an interval-based schedule entry.
    /// </summary>
    /// <param name="schedule">The schedule.</param>
    /// <param name="name">Entry name.</param>
    /// <param name="interval">Execution interval.</param>
    /// <param name="taskName">Task name.</param>
    /// <param name="args">Optional task arguments.</param>
    /// <param name="options">Optional schedule options.</param>
    /// <returns>The schedule for chaining.</returns>
    public static Schedule AddInterval(
        this Schedule schedule,
        string name,
        TimeSpan interval,
        string taskName,
        byte[]? args = null,
        ScheduleOptions? options = null
    )
    {
        schedule.Add(
            new ScheduleEntry
            {
                Name = name,
                Interval = interval,
                Task = new Signature { TaskName = taskName, Args = args },
                Options = options,
            }
        );

        return schedule;
    }

    /// <summary>
    /// Adds a typed cron-based schedule entry.
    /// </summary>
    /// <typeparam name="TTask">The task type.</typeparam>
    /// <typeparam name="TInput">The input type.</typeparam>
    /// <param name="schedule">The schedule.</param>
    /// <param name="name">Entry name.</param>
    /// <param name="cron">Cron expression.</param>
    /// <param name="input">Task input.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="options">Optional schedule options.</param>
    /// <returns>The schedule for chaining.</returns>
    public static Schedule AddCron<TTask, TInput>(
        this Schedule schedule,
        string name,
        string cron,
        TInput input,
        IMessageSerializer serializer,
        ScheduleOptions? options = null
    )
        where TTask : ITask<TInput>
        where TInput : class
    {
        schedule.Add(
            new ScheduleEntry
            {
                Name = name,
                Cron = cron,
                Task = new Signature
                {
                    TaskName = TTask.TaskName,
                    Args = serializer.Serialize(input),
                },
                Options = options,
            }
        );

        return schedule;
    }

    /// <summary>
    /// Adds a typed interval-based schedule entry.
    /// </summary>
    /// <typeparam name="TTask">The task type.</typeparam>
    /// <typeparam name="TInput">The input type.</typeparam>
    /// <param name="schedule">The schedule.</param>
    /// <param name="name">Entry name.</param>
    /// <param name="interval">Execution interval.</param>
    /// <param name="input">Task input.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="options">Optional schedule options.</param>
    /// <returns>The schedule for chaining.</returns>
    public static Schedule AddInterval<TTask, TInput>(
        this Schedule schedule,
        string name,
        TimeSpan interval,
        TInput input,
        IMessageSerializer serializer,
        ScheduleOptions? options = null
    )
        where TTask : ITask<TInput>
        where TInput : class
    {
        schedule.Add(
            new ScheduleEntry
            {
                Name = name,
                Interval = interval,
                Task = new Signature
                {
                    TaskName = TTask.TaskName,
                    Args = serializer.Serialize(input),
                },
                Options = options,
            }
        );

        return schedule;
    }
}
