import pandas as pd
from queries import Query
from sklearn.linear_model import LinearRegression
from sklearn.cross_validation import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns


def scatter(df):
    fig, axs = plt.subplots(3, 3, figsize=(15, 10))
    categories = ['home_adjoe', 'home_adjde', 'away_adjoe', 'away_adjde',
                  'home_poss', 'away_poss', 'home_score', 'away_score', 
                  'margin']

    for idx, ax in enumerate(axs.reshape(-1)):
        ax.scatter(df['id'], df[categories[idx]])
        ax.set_title(categories[idx])

    plt.show(categories[idx])

def hist(df):

    fig, axs = plt.subplots(3, 3, figsize=(15, 10))
    categories = ['home_adjoe', 'home_adjde', 'away_adjoe', 'away_adjde',
                  'home_poss', 'away_poss', 'home_score', 'away_score', 
                  'margin']

    for idx, ax in enumerate(axs.reshape(-1)):
        category = categories[idx]
        ax.hist(df['home_score'], 20)
        ax.set_title(categories[idx])

    plt.show(categories[idx])

def linear(df):
    X = df[['id', 'home_adjoe', 'away_adjde', 'home_poss', 'away_poss']]
    y = df['margin']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
    gameids = X_test[:,0]
    X_test = X_test[:,1:]
    X_train = X_train[:,1:]

    home_score = LinearRegression()
    home_score.fit(X_train, y_train)

    # You can call predict to get the predicted values for training and test
    train_predicted = home_score.predict(X_train)
    test_predicted = home_score.predict(X_test)

    residuals = test_predicted - y_test
    # gameids = gameids[np.where(np.abs(residuals) > 20)]
    test = pd.DataFrame({'y_test': pd.Series(y_test), 
                         'id': pd.Series(gameids),
                         'pred': pd.Series(test_predicted),
                         'resid': pd.Series(residuals)})
    newdf = pd.merge(df, test, on='id')
    newdf = newdf[['home_team', 'away_team', 'home_score', 'away_score', 'margin', 'dt', 'pred', 'resid']]
    print newdf[newdf['resid'].abs() > 20]
    # print newdf.head()
    newdf = df[df['id'].isin(gameids)]
    newdf = newdf[['home_team', 'away_team', 'home_score', 'away_score', 'margin', 'dt']]
    newdf['pred'] = test_predicted
    newdf['resid'] = residuals
    # print newdf.head()
    # plt.scatter(range(len(y_test)), residuals)
    plt.hist(residuals, bins=20)
    # plt.show()


def main():
    q = Query()
    q.query = '''SELECT * FROM features2'''
    df = pd.read_sql(q.query, q.conn)
    q.conn.close()

    df['margin'] = df['home_score'] - df['away_score']


    # scatter(df)
    # hist(df)
    linear(df)

    

    # print df.head()
    # df['tempo'] = (df['home_poss'] * df['away_poss']) / df['poss_game']
    # df['home_score'] = (df['home_adjoe'] + df['away_adjde']) / 2 * df['tempo']
    # df['away_score'] = (df['away_adjoe'] + df['home_adjde']) / 2 * df['tempo']

    # X = df[['home_adjoe', 'away_adjde', 'home_poss', 'away_poss']]
    # y = df['home_score']

    # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)

    # home_score = LinearRegression()
    # home_score.fit(X_train, y_train)

    # # You can call predict to get the predicted values for training and test
    # train_predicted = home_score.predict(X_train)
    # test_predicted = home_score.predict(X_test)

    # residual = y_test
    # # plt.scatter(range(len(residual)), residual)
    # # plt.scatter(range(len(residual)), test_predicted, color='red')
    # # print home_score.score(X_test, y_test)
    # # print home_score.score(X_train, y_train)
    # # print home_score.coef_

    # betas = np.dot(np.linalg.pinv(X_train), y_train)
    # y_predict = np.dot(X_test, betas)
    # plt.scatter(range(len(residual)), y_predict - y_test)
    # # plt.scatter(range(len(residual)), y_test, color='red')
    # plt.show()



if __name__ == '__main__':
    main()