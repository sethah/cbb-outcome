import pandas as pd
from queries import Query
from sklearn.linear_model import LogisticRegression
from sklearn.cross_validation import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

class Analysis(object):

    def __init__(self):
        q = Query()
        q.query = '''SELECT * FROM features2'''
        df = pd.read_sql(q.query, q.conn)
        df['margin'] = df['home_score'] - df['away_score']
        df['outcome'] = np.where(df['margin'] > 0, 1, 0)
        q.conn.close()
        self.df = df

    def convert_dates(self, date_col='dt'):
        self.df[date_col] = pd.to_datetime(self.df[date_col])

    def subplot_dims(self, n):
        if n == 0:
            return (0, 0)
        rows = int(round(np.sqrt(n)))
        cols = int(np.ceil(n/rows))

        return (rows, cols)

    def scatter(self, y_cols, x_col):
        rows, cols = self.subplot_dims(len(y_cols))
        fig, axs = plt.subplots(rows, cols, figsize=(15, 10))

        for idx, ax in enumerate(axs.reshape(-1)):
            ax.scatter(self.df[x_col], self.df[y_cols[idx]])
            ax.set_title(y_cols[idx])

        fig.show()

    def hist(self, y_cols):
        rows, cols = self.subplot_dims(len(y_cols))
        fig, axs = plt.subplots(rows, cols, figsize=(15, 10))

        for idx, ax in enumerate(axs.reshape(-1)):
            sns.distplot(self.df[y_cols[idx]], ax=ax)

        fig.show()

    def team_plot(self, team):
        team_df = self.team_df(team)

        fig, axs = plt.subplots(2, 1, figsize=(10, 15))

        ax = axs[0]
        ax.plot(team_df['team_ngames'], team_df['team_adjoe'], label=team)
        ax.plot(team_df['team_ngames'], team_df['team_adjde'], label='opp')

        ax = axs[1]
        colors = np.where(team_df['team_margin'] > 0, 'green', 'red')
        ax.scatter(team_df['team_adjoe'] * team_df['opp_adjde'],
                   team_df['team_adjde'] * team_df['opp_adjoe'],
                   label=team, color=colors, s=50)
        ax.set_xlabel('Expected Points Scored')
        ax.set_ylabel('Expected Points Allowed')

        plt.legend(loc='best')
        plt.show()

    def team_df(self, team):
        condition = (self.df['home_team'] == team) | \
                    (self.df['away_team'] == team)
        team_df = self.df[condition]

        convert_cols = ['adjoe','adjde','ngames','poss','score']
        for col in convert_cols:
            team_df['team_'+col] = self.team_column(team_df, team, col)
            team_df['opp_'+col] = self.team_column(team_df, team, col, opp=True)
            team_df = team_df.drop(['home_'+col, 'away_'+col], 1)
        team_df['team_margin'] = np.where(team_df['home_team'] == team, team_df['margin'], -1*team_df['margin'])

        return team_df

    def team_column(self, df, team, col_name, opp=False):
        if opp:
            return np.where(
                df['home_team'] == team, 
                df['away_'+col_name], 
                df['home_'+col_name])
        else:
            return np.where(
                df['home_team'] == team, 
                df['home_'+col_name], 
                df['away_'+col_name])

    def logistic(self):
        logistic = LogisticRegression()
        X = self.df[['home_adjoe', 'away_adjoe', 'home_adjde', 'away_adjde',
                     'home_ngames', 'away_ngames', 'home_poss', 'away_poss']]
        y = self.df['outcome']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
        
        logistic.fit(X_train, y_train)
        predict = logistic.predict(X_test)

        self.df['predicted'] = logistic.predict(X)
        
        print 'Accuracy: ', self.accuracy(y_test, predict)
        print 'Precision: ', self.precision(y_test, predict)
        print 'Recall: ', self.recall(y_test, predict)

    def accuracy(self, y_true, y_pred):
        return np.sum(y_true == y_pred) * 1.0 / len(y_true)

    def precision(self, y_true, y_pred):
        true_positives = ((y_pred == 1) & (y_true == 1 )).sum()
        false_positives = ((y_pred == 1) & (y_true != 1 )).sum()
        return true_positives/float(false_positives + true_positives)

    def recall(self, y_true, y_pred):
        true_positives = ((y_pred == 1) & (y_true == 1 )).sum()
        return true_positives/float(np.sum(y_true))

    def plot_predict(self):
        fig, axs = plt.subplots(1, 1, figsize=(10, 15))

        df = self.df[(self.df['home_team'] == 'indiana') |(self.df['away_team'] == 'indiana')]

        ax = axs
        colors = np.where(df['outcome'] == df['predicted'], 'green', 'red')
        ax.scatter(df['home_adjoe'] * df['away_adjde'],
                   df['home_adjde'] * df['away_adjoe'],
                   color=colors, s=50)
        ax.set_xlabel('Expected Points Scored')
        ax.set_ylabel('Expected Points Allowed')

        plt.legend(loc='best')
        plt.show()

    def home_away(self):
        fig, axs = plt.subplots(1, 1, figsize=(10, 15))
        ax=axs
        sns.distplot(self.df['home_score']/self.df['home_poss'], ax=ax, label='Home')
        sns.distplot(self.df['away_score']/self.df['away_poss'], ax=ax, label='Away')
        ax.set_xlabel('Score')

        plt.legend(loc='best')

        

        # fig, axs = plt.subplots(1, 1, figsize=(10, 15))
        # ax = ax
        # sns.jointplot(self.df['home_score'], self.df['away_score'], kind='hex')

        plt.show()
        


def main():
    a = Analysis()
    a.df.sort('dt', ascending=True)
    a.convert_dates()
    scatter_cols = ['home_adjoe', 'home_adjde', 'away_adjoe', 'away_adjde',
                      'home_poss', 'away_poss', 'home_score', 'away_score', 
                      'margin']
    y_col = 'date_int'
    a.df['date_int'] = a.df.dt.astype(np.int64)
    # a.scatter(scatter_cols, y_col)
    # a.hist(scatter_cols)
    # a.team_plot('indiana')
    # a.test_seaborn('indiana')
    # a.logistic()
    # a.plot_predict()
    a.home_away()

if __name__ == '__main__':
    main()