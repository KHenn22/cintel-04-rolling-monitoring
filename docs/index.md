# Continuous Intelligence

This site provides documentation for this project.
Use the navigation to explore module-specific materials.

## How-To Guide

Many instructions are common to all our projects.

See
[⭐ **Workflow: Apply Example**](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to get these projects running on your machine.

## Project Documentation Pages (docs/)

- **Home** - this documentation landing page
- **Project Instructions** - instructions specific to this module
- **Your Files** - how to copy the example and create your version
- **Glossary** - project terms and concepts

## Additional Resources

- [Suggested Datasets](https://denisecase.github.io/pro-analytics-02/reference/datasets/cintel/)

## Custom Project

### Dataset
T_ONTIME_REPORTING_OCT_2025.csv — a Bureau of Transportation Statistics (BTS) dataset containing 605,844 individual flight records for October 2025, covering 14 U.S. airlines with columns for departure delays, cancellations, carrier codes, and flight dates.

### Signals
Three daily signals were computed per airline: flights (total flight count), cancellations (sum of cancelled flights), and avg_dep_delay (mean departure delay in minutes). Rolling means and standard deviations were calculated over a 5-day rolling window throughout October by carrier using .over("OP_UNIQUE_CARRIER").

### Experiments
The original rolling window size in the technical modification part of the assignment was increased from 3 to 5 days, and the anomaly threshold was reduced from 2 standard deviations down to 1 to increase sensitivity. Grouping was by each airline one at a time through the entire 31 days.

### Results
87 delay spikes were flagged across all 14 airlines for October 2025.  United, Republic, Delta, and American had the most delay spikes (8 each), followed by Jet Blue, Frontier, and Southwest (7), Alaska and Envoy (6 each). SkyWest had the fewest with 3 flagged days.

77 cancellation spikes were flagged across all 14 airlines for October 2025.  Delta (9) led, with Alaska, Hawaiian, United, and American next (7 each).  Allegiant had the fewest at 2 cancellations.

### Interpretation
Airlines exceeding their own rolling average by 1 standard deviation signal operationally abnormal days. United, Republic, Delta and American showing 8 spike days out of 31 suggests roughly 1 in 4 days had unusually high delays. This is useful for identifying carriers with inconsistent on-time performance rather than just high average delays.​​​​​​​​​​​​​​​​  However, it should be mentioned that a monthly data set is far too small to determine if an airline has an overall issue with operational efficiency.
