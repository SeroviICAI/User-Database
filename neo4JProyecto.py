from utils.database import connect_to_neo4j, connect_to_mongodb, connect_to_mysql


NEO_DRIVER = connect_to_neo4j()
MONGO_CLIENT = connect_to_mongodb()
MYSLQ_CONN = connect_to_mysql()

# Connecting to our databases
nom_bd = 'amz_reviews'
MYSLQ_CONN.database = nom_bd

nom_coll = 'reviews'
collection = MONGO_CLIENT[nom_bd][nom_coll]


def take_a_number(message: str)-> int:
    '''
    Will take a number from keyboard
    '''
    num = 0

    while not num:
        try:
            num = int(input(message))
        except ValueError:
            print('Enter a number')
            return take_a_number(message)
        
    return num


def section_1():
    '''
    Will perform section 2 of neo4j

    Returns:
        user with the most relationships
    '''
    # We ask for the number of users
    n_users = take_a_number('Enter number of users to analyze: ')
    # We will define the Jaccard similarity

    def jaccard_similarity(set1, set2):
        '''
        Calculates the Jaccard similarity for two sets

        '''
        intersection = set1.intersection(set2)
        union = set1.union(set2)
        return len(intersection) / len(union)

    # And a query to clean the DB
    delete_query = "MATCH (n) DETACH DELETE n"

    # To obtain the users, we must connect to MongoDB
    r = list(collection.aggregate([
        {'$group': {'_id': '$reviewer_id', 'rev_amount': {'$sum': 1}}},
        {'$sort': {'rev_amount': -1}},
        {'$project': {'rev_amount': 0}},
        {'$limit': n_users}
        ]
    ))

    # Now, for each user, let's see which items they have rated
    # We will save everything in a dictionary, with key -> user, value -> set with voted items
    # At the same time, we will calculate the similarity of the current user with that of the previous ones
    # If there is similarity, we will save a tuple (user1, user2, similarity)
    users = {}
    similarity = []
    for user in r:
        user = user['_id']
        items = list(collection.find({'reviewer_id': user}, {'item_id': 1, '_id': 0}))
        items = set(art.get('item_id') for art in items)

        for user_2, set_2 in users.items():
            jaccard = jaccard_similarity(items, set_2)
            if jaccard:
                similarity.append((user, user_2, round(jaccard, 4)))
        # Finally, we add the current user
        users[user] = items

    # Let's start with the neo4j queries
    users_query = """
                       CREATE (n:REVIEWER {id: $id}) RETURN n;
                       """

    similarity_query = """
                         MATCH (user_1:REVIEWER {id: $id_user_1} ),
                         (user_2:REVIEWER {id: $id_user_2})
                         CREATE (user_1) - [:SIMILAR_TO {jaccard: $jaccard}] -> (user_2)
                         CREATE (user_2) - [:SIMILAR_TO {jaccard: $jaccard}] -> (user_1)
                         """

    neigh_query = """
                       MATCH (u:REVIEWER)-[r:SIMILAR_TO]->(:REVIEWER)
                       RETURN u, count(r) as similars
                       ORDER BY similars DESC
                       LIMIT 1
                       """

    with NEO_DRIVER.session() as session:
        session.run(delete_query)
        # We create users
        for user in users.keys():
            session.run(users_query, id=user)
        # We create relations
        for user_1, user_2, jaccard in similarity:
            session.run(similarity_query, id_user_1=user_1, id_user_2=user_2, jaccard=jaccard)
        # We search the user with most neighbours
        most_neigh = session.run(neigh_query)
        data = most_neigh.data()[0]

    message = f"Data loaded.\nThe user with most neighbours is user \
'{data['u']['id']}', which has {data['similars']} neighbours"
    print(message)
    return data


def section_2():
    '''
    Will perform section 2 of neo4j
    '''
    def take_categories(cursor, ab_cat=False):
        '''
        Will show actual categories from the database, and will ask for some of them
        '''
        if not ab_cat:
            categories_query = '''
                            SELECT DISTINCT category
                            FROM items 
                            '''
            cursor.execute(categories_query)
            ab_cat = list(d[0] for d in cursor.fetchall())

        print('\nAvailable categories are: ')
        for i in range(len(ab_cat)):
            print(f'{i + 1}. {ab_cat[i]}')

        message = 'Choose the categories: (format cat1-cat2...),\
for example "1-4"\n'
        categories = input(message)
        try:
            categories = list(map(int, categories.split('-')))
            categories = [ab_cat[i - 1] for i in categories]
        except (ValueError, IndexError):
            print('\nIncorrect type or category')
            return take_categories(cursor, ab_cat)

        return categories

    n = take_a_number('Enter the number of aleatory items to get: ')
    cursor = MYSLQ_CONN.cursor()
    categories = take_categories(cursor)
    sql_query = '''
            SELECT id
            FROM items
            WHERE category IN ({})
            ORDER BY RAND()
            LIMIT {}
            '''.format(','.join(['%s'] * len(categories)), n)
    cursor.execute(sql_query, categories)
    items = cursor.fetchall()
    cursor.close()

    # Once we have the items, let's collect which users have reviewed them
    # We will save this in a dictionary art:{reviewers: [], overall: [], reviewTime: []},
    # as well as a set with users
    reviews = {}
    users = set()
    for art in items:
        art = art[0]
        # We look for the users
        r  = list(collection.find({'item_id': art}, {'reviewer_id': 1, 'overall': 1, 'reviewTime':1,  '_id':0}))
        user = list(user.get('reviewer_id') for user in r)
        users.update(user)
        reviews[art] = {
                        'reviewers': user,
                        'overall': list(user.get('overall') for user in r),
                        'reviewTime': list(user.get('reviewTime') for user in r)
                        }

    # Once we have the structure, we can add it to neo4j
    # query to clean the database
    delete_query = "MATCH (n) DETACH DELETE n"

    item_query = """
                    CREATE (n:ITEM {id: $id}) RETURN n;
                    """
    users_query = """
                       CREATE (n:REVIEWER {id: $id}) RETURN n;
                       """

    reviews_query = """
                       MATCH (user:REVIEWER {
                           id: $user_id
                       }), (item:ITEM {
                           id: $item_id
                       })
                       CREATE (user)-[:REVIEWED {
                           overall: $overall,
                           reviewTime: $reviewTime
                       }]->(item)
                       """

    with NEO_DRIVER.session() as session:
        # We clean the database
        session.run(delete_query)
        # We add the items
        for art in items:
            art = art[0]
            session.run(item_query, {'id': art})
        # We add the users
        for user in users:
            session.run(users_query, {'id': user})
        # We add the reviews
        for art in reviews:
            for i in range(len(reviews[art]['reviewers'])):
                session.run(reviews_query, {
                    'user_id': reviews[art]['reviewers'][i],
                    'item_id': art,
                    'overall': reviews[art]['overall'][i],
                    'reviewTime': reviews[art]['reviewTime'][i]
                })

    print('Data loaded correctly')
    return


def section_3():
    '''
    Will perform section 3 of neo4j
    '''
    # First, we will have to collect the users from mysql
    n_users = take_a_number('Enter the number of users to select: ')
    sql_users = f'''
                    SELECT id
                    FROM users
                    ORDER BY ISNULL(reviewerName), reviewerName
                    LIMIT {n_users}
                    '''
    
    cursor = MYSLQ_CONN.cursor()
    cursor.execute(sql_users)
    users = list(d[0] for d in cursor.fetchall())
    cursor.close()

    # For each user, we'll se how many reviews of each category has
    r = list(collection.aggregate([
                {'$match': {'reviewer_id': {'$in': users}}},
                {'$group':{'_id': {'user': '$reviewer_id', 'category': '$category'}, 'count': {'$sum': 1}}},
                {'$group':{
                            '_id': '$_id.user', 'categories': {'$push': {'name': '$_id.category', 'count': '$count'}},
                           'num_categories': {'$sum': 1}}
                        },
                {'$match': {'num_categories': {'$gte': 2}}},
                {'$sort': {'_id': 1}}
            ]))

    # And now we process the information in a convenient way

    total_categories = set()
    # Will follow the structure user: {categories: [], count: []}
    user_categories = {}
    for user in r:
        cat = []
        count = []
        user_id = user.get('_id')
        categories = user.get('categories')
        for categoria in categories:
            tmp = categoria.get('name')
            total_categories.add(tmp)
            cat.append(tmp)
            count.append(categoria.get('count'))
        user_categories[user_id] = {'categories': cat, 'count': count}

    # Once we have the structure, we can add it to neo4j
    delete_query = "MATCH (n) DETACH DELETE n"          # query to clean the DB

    category_query = """
                        CREATE (n:CATEGORY {id: $id}) RETURN n;
                        """
    users_query = """
                       CREATE (n:REVIEWER {id: $id}) RETURN n;
                       """

    reviews_query = """
                       MATCH (user:REVIEWER {id: $id_user} ),
                       (cat:CATEGORY {id: $id_cat})
                       CREATE (user) - [:REVIEWED {times: $times}] -> (cat)
                       """

    with NEO_DRIVER.session() as session:
        session.run(delete_query)
        # We create items and users
        for category in total_categories:
            session.run(category_query, id=category)
        for user in user_categories.keys():
            session.run(users_query, id=user)
        # We create relations
        for user, info in user_categories.items():
            for i in range(len(info['categories'])):
                session.run(
                            reviews_query,
                            id_user=user,
                            id_cat=info['categories'][i],
                            times=info['count'][i]
                        )

    print('Data loaded correctly')
    return


def section_4():
    '''
    Will perform section 4 of neo4j
    '''
    n_items = take_a_number('Enter the number of items to select: ')
    # Let's see which items are the most popular meeting the requirements

    pop_items = list(collection.aggregate([
                {'$group':{'_id': '$item_id', 'count': {'$sum': 1}}},
                {'$match': {'count': {'$lt': 40}}},
                {'$sort': {'count': -1}},
                {'$limit': n_items}
            ]))
    pop_items = [item['_id'] for item in pop_items]

    # Now we will collect all the users who have voted for any of these
    item_usr = {}
    total_usr = set()
    for item in pop_items:
        users = list(collection.find({'item_id': {'$in': [item]}}, {'reviewer_id': 1, '_id': 0}))
        users = [user['reviewer_id'] for user in users]
        item_usr[item] = users
        total_usr.update(users)

    # Let's analyze relationships
    # We create a dictionary to store the elements voted by each user
    usr_items = {user: set() for user in total_usr}

    # We go through the item_usr dictionary and add each element to its respective set of users

    raw_usr_items = list(collection.aggregate([
                            {'$match': {'reviewer_id': {'$in': list(total_usr)}}},
                            {'$group': {'_id': {'user': '$reviewer_id', 'item': '$item_id'}}}
                        ]))

    for element in raw_usr_items:
        user = element['_id']['user']
        item = element['_id']['item']
        usr_items[user].add(item)

    # We will store the relationships in the form (user_1, user_2, common)
    common_items = []

    # We compare the voted elements
    for user_1 in total_usr:
        for user_2 in total_usr:
            if user_1 != user_2:
                common = usr_items[user_1].intersection(usr_items[user_2])
                # If they exist, we'll save them
                if common:
                    common_items.append((user_1, user_2, len(common)))

    # Once we have the structure, we can add it to neo4j
    # query to clean the DB
    delete_query = "MATCH (n) DETACH DELETE n"

    consulta_item = """
                    CREATE (n:ITEM {id: $id}) RETURN n;
                    """
    users_query = """
                       CREATE (n:REVIEWER {id: $id}) RETURN n;
                       """

    reviews_query = """
                       MATCH (user:REVIEWER {id: $id_user}),
                       (item:ITEM {id: $id_item})
                       CREATE (user) - [:REVIEWED] -> (item)
                       """
    common_query = """
                     MATCH (user_1: REVIEWER {id: $id_user_1}),
                     (user_2: REVIEWER {id: $id_user_2})
                     CREATE (user_1) - [:COMMON {cantidad: $cantidad}] -> (user_2)
                     """

    with NEO_DRIVER.session() as session:
        session.run(delete_query)
        # We create items and users
        for item in pop_items:
            session.run(consulta_item, id=item)
        for user in total_usr:
            session.run(users_query, id=user)
        # We create relations
        for item, users in item_usr.items():
            for user in users:
                session.run(
                            reviews_query,
                            id_user=user,
                            id_item=item,
                        )
        for user_1, user_2, cant in common_items:
            session.run(common_query, id_user_1=user_1, id_user_2=user_2, cantidad=cant)

    print('Data loaded correctly')
    return


def menu():
    '''
    Interactive menu for users to choose the section they want to execute
    '''
    sections = {'1': section_1, '2': section_2, '3': section_3, '4': section_4, '5': exit}

    print('Welcome to neo4J proyect. You can choose a section to be executed.')
    print("If needed, you'll be asked for some inputs")

    message = "Choose a section:\n1. Users similitudes\n2. User-item relations\
    \n3. Users-Categories\n4. Popular items and relations between users\
    \n5. End program\n"

    while True:
        try:
            section = input(message)
            while section not in sections.keys():
                print('\nEnter a valid option')
                section = input(message)
            sections[section]()
            print('\nGoing back to menu. . .\n\n')
        except KeyboardInterrupt:
            print('Procces stopped. To exit, press 5')


if __name__ == '__main__':
    menu()
