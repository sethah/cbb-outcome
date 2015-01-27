import pandas as pd
from queries import Query
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.cross_validation import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import plotly
import plotly.plotly as py  
import plotly.tools as tls

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

    def adjusted_labels(self, labels):
        adj_cols = ['adj_'+item for item in labels] + \
                   ['adj_d'+item for item in labels]
        
        return adj_cols

    def prefix_cols(self, cols, prefix='loc'):
        if prefix == 'loc':
            prefix1 = 'home'
            prefix2 = 'away'
        elif prefix == 'team':
            prefix1 = 'team'
            prefix2 = 'away'

        cols = [prefix1+'_'+col for col in cols] + \
               [prefix2+'_'+col for col in cols]

        return cols

    def get_X(self):
        adj_factors = self.adjusted_labels(['ppp', 'trt', 'efg', 'ftr'])
        raw_factors = adj_factors + ['ngames', 'poss', 'pts_scaled']
        factors = self.prefix_cols(raw_factors)

        return self.df[factors]

    def subplot_dims(self, n):
        if n == 0:
            return (0, 0)
        rows = int(round(np.sqrt(n)))
        cols = int(np.ceil(n/rows))

        return (rows, cols)

    def scatter(self, y_cols, x_col):
        # if 1:
        def onclick(event):
            ind = np.array(event.ind)
            l = zip(self.df['home_team'][ind].values,
                      self.df['home_score'][ind].values,
                      self.df['away_team'][ind].values,
                      self.df['away_score'][ind].values)
            for item in l:
                print item
        rows, cols = self.subplot_dims(len(y_cols))
        fig, axs = plt.subplots(rows, cols, figsize=(10, 8))

        for idx, ax in enumerate(axs.reshape(-1)):
            ax.scatter(self.df[x_col], self.df[y_cols[idx]], picker=True)
            ax.set_title(y_cols[idx])


        

        fig.canvas.mpl_connect('pick_event', onclick)

        plt.show()
        # mp = plt.gcf()
        # pyfig = tls.mpl_to_plotly(mp)
        # py.iplot_mpl(mp)

    def hist(self, y_cols):
        rows, cols = self.subplot_dims(len(y_cols))
        fig, axs = plt.subplots(rows, cols, figsize=(15, 10))

        for idx, ax in enumerate(axs.reshape(-1)):
            sns.distplot(self.df[y_cols[idx]], ax=ax)

        fig.show()

    def correlation_plot(self):
        X = self.get_X()

        cmap = sns.diverging_palette(220, 10, as_cmap=True)
        sns.corrplot(X, annot=False, sig_stars=False,
                     diag_names=False, cmap=cmap)
        plt.show()

    def team_plot(self, team):
        team_df = self.team_df(team)

        fig, axs = plt.subplots(2, 2, figsize=(10, 15))

        adj_cols = ['ppp', 'trt', 'efg', 'ftr']

        for idx, ax in enumerate(axs.reshape(-1)):
            ax.plot(team_df['team_ngames'], team_df['team_adj_'+adj_cols[idx]], label='offensive')
            ax.plot(team_df['team_ngames'], team_df['team_adj_d'+adj_cols[idx]], label='defensive')
            ax.set_title(adj_cols[idx])
            ax.legend(loc='best')
        
        plt.show()

    def team_df(self, team):
        condition = (self.df['home_team'] == team) | \
                    (self.df['away_team'] == team)
        team_df = self.df[condition]

        adj_cols = ['ppp', 'trt', 'efg', 'ftr']
        convert_cols = ['adj_'+item for item in adj_cols] + ['adj_d'+item for item in adj_cols]
        convert_cols += ['ngames','poss','score']

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

    def discrete_predict(self):
        X = X = self.get_X()
        y = self.df['outcome']

        # split the data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)

        rf = RandomForestClassifier()
        # rf_predict = rf.fit(X_train, y_train).predict(X_test)
        log = LogisticRegression()
        # log_predict = log.fit(X_train, y_train).predict(X_test)

        rf_precisions = np.array(cross_val_score(rf, X_train, y_train, cv=10, scoring='precision'))
        rf_accuracies = np.array(cross_val_score(rf, X_train, y_train, cv=10, scoring='accuracy'))
        rf_recalls = np.array(cross_val_score(rf, X_train, y_train, cv=10, scoring='recall'))
        log_precisions = np.array(cross_val_score(log, X_train, y_train, cv=10, scoring='precision'))
        log_accuracies = np.array(cross_val_score(log, X_train, y_train, cv=10, scoring='accuracy'))
        log_recalls = np.array(cross_val_score(log, X_train, y_train, cv=10, scoring='recall'))

        print 'Accuracy: %s (RF), %s (Logistic) ' % (np.mean(rf_accuracies), np.mean(log_accuracies))
        print 'Precision: %s (RF), %s (Logistic) ' % (np.mean(rf_precisions), np.mean(log_precisions))
        print 'Recall: %s (RF), %s (Logistic) ' % (np.mean(rf_recalls), np.mean(log_recalls))

    def continuous_predict(self):
        X = X = self.get_X()
        y = self.df['margin']

        # split the data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)

        rf = RandomForestClassifier()
        rf_predict = rf.fit(X_train, y_train).predict(X_test)
        lin = LinearRegression()
        lin_predict = lin.fit(X_train, y_train).predict(X_test)

        sns.distplot(rf_predict - y_test, label='Random Forest')
        sns.distplot(lin_predict - y_test, label='Linear Regression')

        plt.legend(loc='best')
        plt.show()

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
        ax.scatter(df['home_adj_ppp'] * df['away_adj_dppp'],
                   df['home_adj_dppp'] * df['away_adj_ppp'],
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

        plt.show()
        

def onpick3(event, x, y):
        ind = event.ind
        print 'onpick3 scatter:', ind, np.take(x, ind), np.take(y, ind)

def main():
    a = Analysis()
    a.df.sort('dt', ascending=True)
    a.df = a.df.dropna()
    a.convert_dates()
    a.df['date_int'] = a.df.dt.astype(np.int64)
    # print a.df.info()
    # return None
    
    a.df['home_pts_scaled'] = a.df['home_adj_ppp'] * a.df['away_adj_dppp']
    a.df['away_pts_scaled'] = a.df['away_adj_ppp'] * a.df['home_adj_dppp']
    # a.correlation_plot()
    # return None
    # return None
    scatter_cols = ['home_adj_ppp', 'home_adj_dppp', 'away_adj_ppp', 'away_adj_dppp',
                      'home_poss', 'away_poss', 'home_score', 'away_score', 
                      'margin']
    # y_col = 'date_int'
    a.scatter(y_cols=scatter_cols, x_col='date_int')
    

if __name__ == '__main__':
    main()