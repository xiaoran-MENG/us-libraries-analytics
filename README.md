# COMP 3380 Group Project
## US Libraries Analytics

#### Group Members
Xiaoran Meng, Ethan, Frieda

#### Live Demo
![](demo.gif)

#### Running the Analytics
1. Run `make` from the project folder
2. Run `make run` if step 1 is successful

#### Execution Process
1. A popup is displayed to let the user choose whether or not to seed the database
    ![](seed-the-database.png)
    _Note: if you close the popup without having the database seeded, the database will **automatically** get seeded after you choose your first report to display_
2. The reports directory is displayed in the terminal, waiting on the user to enter a report index
    ![](reports-directory.png)
3. The analytics then runs the corresponding query executor to fetch data for the selected report
    _Note: if the user chooses a report that requires user-defined parameters, a form is displayed to take in the parameters from the user. `Mousing over` the text fields would trigger display of `parameter name` and a `suggested value`_
    ![](args-form.png)
4. The search results are tabulated with a scrollbar on the right
    ![](table.png)

5. Repeat `2` until the user enters `q` 
    ![](thank-you.png)

#### FAQ
1. Why is my analytics throwing UI exceptions at startup ?
_The desktop version of the analytics DOES work in the Linux Lab. So try to log into one of the Linux machine in the Linux Lab and run it. Alternatively, a backup Terminal version of the analytics `AppBackup.java` under `/backup` is provided. You just need to move it to the project folder and update the `makefile` to operate on `AppBackup.java`_