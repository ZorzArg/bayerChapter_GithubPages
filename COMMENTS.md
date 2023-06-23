# Bayer Chapter

## Meta Info

-   Developer: George Argyriou
-   Reviewer: Martin Lavallee

## Comments

Overall this is a good first effort! I am glad you got this to work and we have improved our codebase since we started this project. The biggest issue is documentation and organization. As a team we need to be better about commenting code and providing documentation.

Please re-write this code to fit the Ulysses structure. I have started to do some of this and will continue to support. Once the re-write is done, test and debug it on the smallest database to ensure it runs correctly. This is important because we need more examples to use for Ulysses and build our codebase in a cohesive manner. It is important that the client sees code of subsequent projects in a consistent manner.

### Major

-   Reformat to Ulysses structure, see ulysses branch in repo for guidance.
-   Need to improve documentation. The README file has not changed since it defaults, the NEWS file has little information. These need to be more effort towards the documentation of the study. Add in the Ulysses format to the analysis scripts so they read a bit better. Add comments to the code to explain what is happening in the script.
-   Missing a cohort details file for the 5 analytical cohorts. Please create a simple version of this document using Ulysses
-   Please remove any interactive commands. There should be no `View` or `debug` commands in the code. That stuff is for you not for the repo. Repo code should be production grade.
-   Steps 8 and 9 have the same name. What is the difference? Please rename them to more specific analytical tasks
-   Output files should be saved as csv and not parquet. This is something we changed midway so ok but change on the rewrite.\
-   Add a HowToRun file to explain how the project should be deployed. I have started to build this in the `ulysses` branch and will help you.

### Minor

-   I prefer avoiding for-loops they are hard to read. Consider permuting your analyses prior to entering them into functions. You should fit a rectangular settings object into a function. Ulysses adds a settings folder to help with this. Save any settings files as csv.
-   The scriptTasks should be very readable and short. Leave your heavy lifting to the private functions. You do this sometimes but not always. We changed our approach in strata which would be the biggest change

### Suggestions

-   The code works and you are already developing the shiny so no need to change too much. However, it is very important that the analysis scripts are very legible. They need to flow through the analytic. Place more code in internals and less code in the script itself.
-   We need to mature the modules created in the last few studies so we can reuse the same set of code.
