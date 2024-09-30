from __future__ import annotations
from flask import Flask, request, jsonify 
from connect_connector import connect_with_connector
from flask import Flask, request
import logging
import os
import sqlalchemy
from sqlalchemy import text
from google.cloud.sql.connector import Connector, IPTypes

# Set up variables
BUSINESS = 'Business'
ERORR_NOT_FOUND = {"Error" : "No business with this business_id exists"}
ERORR_NOT_FOUND_REVIEW = {"Error" : "No review with this review_id exists"}

app = Flask(__name__)
logger = logging.getLogger()

# Sets up connection pool for the app
def init_connection_pool() -> sqlalchemy.engine.base.Engine:
    if os.environ.get('INSTANCE_CONNECTION_NAME'):
        return connect_with_connector()
        
    raise ValueError(
        'Missing database connection type. Please define INSTANCE_CONNECTION_NAME'
    )

# This global variable is declared with a value of `None`
db = None

# Initiates connection to database
def init_db():
    global db
    db = init_connection_pool()

# create 'businesses' table in database if it does not already exist
def create_table(db: sqlalchemy.engine.base.Engine) -> None:
    with db.connect() as conn:
        conn.execute(
            sqlalchemy.text(
                '''
                CREATE TABLE IF NOT EXISTS businesses (
                    business_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    owner_id INT NOT NULL,
                    name VARCHAR(50) NOT NULL,
                    street_address VARCHAR(100) NOT NULL,
                    city VARCHAR(50) NOT NULL,
                    state VARCHAR(2) NOT NULL,
                    zip_code INT NOT NULL
                );
                '''
            )
        )
        conn.commit()

@app.route('/')
def index():
    return 'Please navigate to /businesses to use this API'

# Create a new business 
@app.route('/businesses', methods=['POST'])
def create_business():

    content = request.get_json()
    required_fields = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']
    # Error handle if we are missing any fields
    if not all(field in content for field in required_fields):
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400
    # Connect to the database and insert the new business
    try:
        with db.connect() as conn:
            stmt = sqlalchemy.text(
                'INSERT INTO businesses(owner_id, name, street_address, city, state, zip_code) '
                'VALUES (:owner_id, :name, :street_address, :city, :state, :zip_code)'
            )
            conn.execute(stmt, parameters={'owner_id': content['owner_id'], 
                                        'name': content['name'], 
                                        'street_address': content['street_address'],
                                        'city': content['city'],
                                        'state': content['state'],
                                        'zip_code': content['zip_code']})
            conn.commit()

            # Get the ID of the new business
            business_id = conn.execute(text('SELECT LAST_INSERT_ID()')).scalar()

            business_url = f'{request.url_root}businesses/{business_id}'
            return ({'id': business_id,
                            'owner_id': content['owner_id'],
                            'name': content['name'],
                            'street_address': content['street_address'],
                            'city': content['city'],
                            'state': content['state'],
                            'zip_code': content['zip_code'], 
                            'self': business_url}), 201
    except Exception as e:
        logger.exception(e)
        return ('Error:', 'Unable to create business'), 500
    
#Return all businesses
@app.route('/businesses', methods=['GET'])
def get_businesses():
    # Set offsets and limits for pagination
    try:
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', 3)) 
    except ValueError:
        return jsonify({"Error": "Invalid query parameter"}), 400

    with db.connect() as conn:
        # Query to fetch businesses with pagination/offset
        query = sqlalchemy.text(
            'SELECT business_id, owner_id, name, street_address, city, state, zip_code '
            'FROM businesses ORDER BY business_id LIMIT :limit OFFSET :offset'
        )
        results = conn.execute(query, {'limit': limit, 'offset': offset}).mappings().all()
        businesses = []

        for business in results:
            business_dict = {
                'id': business['business_id'],  # Changed 'business_id' to 'id'
                'owner_id': business['owner_id'],
                'name': business['name'],
                'street_address': business['street_address'],
                'city': business['city'],
                'state': business['state'],
                'zip_code': business['zip_code'],
                'self': f"{request.url_root}businesses/{business['business_id']}"
            }
            businesses.append(business_dict)

        # Determine if there is a next page
        if len(businesses) == limit:
            next_offset = offset + limit
            next_url = f"{request.url_root}businesses?limit={limit}&offset={next_offset}"
        else:
            next_url = None

        response = {
            'entries': businesses,
            'next': next_url
        }

        return jsonify(response), 200

# Return a single business 
@app.route('/businesses/<int:id>', methods=['GET'])
def get_business(id):
    with db.connect() as conn:
        stmnt = sqlalchemy.text(
            '''
            SELECT business_id, owner_id, name, street_address, city, state, zip_code
            FROM businesses WHERE business_id = :business_id
            '''
        )
        result = conn.execute(stmnt, {'business_id': id}).mappings().one_or_none()

        if result is None:
            return jsonify({"Error": "No business with this business_id exists"}), 404
        else:
            # Construct the response dictionary explicitly in the desired order
            business = {
                'id': result['business_id'],
                'owner_id': result['owner_id'],
                'name': result['name'],
                'street_address': result['street_address'],
                'city': result['city'],
                'state': result['state'],
                'zip_code': result['zip_code'],
                'self': f"http://{request.host}/businesses/{result['business_id']}"
            }
            return jsonify(business), 200

# Edit a business
@app.route('/businesses' + '/<int:id>', methods=['PUT'])
def edit_business(id):
    content = request.get_json()
    required_fields = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']
    # Error handle if we are missing any fields
    if not all(field in content for field in required_fields):
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400
            
    with db.connect() as conn:
        # Check if the business exists
        check_stmnt = sqlalchemy.text(
            'SELECT * FROM businesses WHERE business_id=:business_id'
        )
        
        result = conn.execute(check_stmnt, parameters={'business_id': id}).fetchone()
        if not result:
            return ERORR_NOT_FOUND, 404
        # Update the business
        update_stmnt = sqlalchemy.text(
                'UPDATE businesses '
                'SET owner_id = :owner_id, name = :name, street_address = :street_address, city = :city, state = :state, zip_code = :zip_code '
                'WHERE business_id = :business_id'
            )
        conn.execute(update_stmnt, parameters={'owner_id': content['owner_id'],
                                            'name': content['name'],
                                            'street_address': content['street_address'],
                                            'city': content['city'],
                                            'state': content['state'],
                                            'zip_code': content['zip_code'],
                                            'business_id': id})
        conn.commit()
        # Return updated business
        business_url = f'{request.url_root}businesses/{id}'
        updated_business = {
            'id': id,
            'owner_id': content['owner_id'],
            'name': content['name'],
            'street_address': content['street_address'],
            'city': content['city'],
            'state': content['state'],
            'zip_code': content['zip_code'],
            'self': business_url
        }
        return jsonify(updated_business), 200

# Delete a business
@app.route('/businesses' + '/<int:id>', methods=['DELETE'])
def delete_business(id):
    with db.connect() as conn:
        stmnt = sqlalchemy.text(
            'DELETE FROM businesses WHERE business_id=:business_id'
        )
        # Check if the business exists
        result = conn.execute(stmnt, parameters={'business_id': id})
        conn.commit()
        if result.rowcount == 1:
            return ('', 204)
        else:
            return ERORR_NOT_FOUND, 404
    
        
# List all Bussiness for an owner
@app.route('/owners' + '/<int:owner_id>' + '/businesses', methods=['GET'])
def get_owners_businesses(owner_id):
    with db.connect() as conn:
        # Select all businesses for the specified owner
        query = sqlalchemy.text(
            "SELECT business_id, owner_id, name, street_address, city, state, zip_code "
            "FROM businesses WHERE owner_id = :owner_id"
        )
        results = conn.execute(query, {'owner_id': owner_id}).mappings().all()

        if not results:
            return jsonify({"Error": "No businesses found for this owner_id"}), 404
        # Construct dictionary of businesses
        businesses = []
        for business in results:
            business_dict = {
                'id': business['business_id'],  # Changed 'business_id' to 'id'
                'owner_id': business['owner_id'],
                'name': business['name'],
                'street_address': business['street_address'],
                'city': business['city'],
                'state': business['state'],
                'zip_code': business['zip_code'],
                'self': f"{request.url_root}businesses/{business['business_id']}"
            }
            businesses.append(business_dict)

        return jsonify(businesses), 200

###########################################################################
#                                                                         #
#                                REVIEWS                                  #
#                                                                         #
###########################################################################

# create reviews table in database if it does not already exist
def create_reviews_table(db: sqlalchemy.engine.base.Engine) -> None:
    try:
        with db.connect() as conn:
            conn.execute(
                sqlalchemy.text(
                    '''
                    CREATE TABLE IF NOT EXISTS reviews (
                        review_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                        business_id BIGINT UNSIGNED NOT NULL,
                        user_id INT NOT NULL,
                        stars INT NOT NULL,
                        review_text VARCHAR(1000),
                        FOREIGN KEY (business_id) REFERENCES businesses(business_id) ON DELETE CASCADE
                    );
                    '''
                )
            )
            conn.commit()
    except Exception as e:
        logger.error("Failed to create reviews table: %s", str(e))

# Create a new review
@app.route('/reviews', methods=['POST'])
def create_review():
    content = request.get_json()
    # Check if all required fields are present
    required_fields = ['user_id', 'business_id', 'stars']
    if not all(field in content for field in required_fields):
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400

    with db.connect() as conn:
        try:
            # Check if the specified business exists
            business_exists = conn.execute(
                sqlalchemy.text("SELECT 1 FROM businesses WHERE business_id = :business_id"),
                {'business_id': content['business_id']}
            ).scalar()

            if not business_exists:
                return jsonify({"Error": "No business with this business_id exists"}), 404

            # Check for an existing review by the same user for the same business
            review_exists = conn.execute(
                sqlalchemy.text("SELECT 1 FROM reviews WHERE user_id = :user_id AND business_id = :business_id"),
                {'user_id': content['user_id'], 'business_id': content['business_id']}
            ).scalar()

            if review_exists:
                return jsonify({"Error": "You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review"}), 409

            # Insert the new review
            review_text = content.get('review_text', '')
            conn.execute(
                sqlalchemy.text(
                    "INSERT INTO reviews (user_id, business_id, stars, review_text) VALUES (:user_id, :business_id, :stars, :review_text)"
                ),
                {'user_id': content['user_id'], 'business_id': content['business_id'], 'stars': content['stars'], 'review_text': review_text}
            )
            review_id = conn.execute(sqlalchemy.text("SELECT LAST_INSERT_ID()")).scalar()
            conn.commit()

            # return the new review
            review_url = f"{request.url_root}reviews/{review_id}"
            business_url = f"{request.url_root}businesses/{content['business_id']}"
            return jsonify({
                'id': review_id,
                'user_id': content['user_id'],
                'business': business_url,
                'stars': content['stars'],
                'review_text': review_text,
                'self': review_url
            }), 201

        except Exception as e:
            logger.exception("Failed to create review: %s", e)
            conn.rollback()
            return jsonify({'Error': 'Unable to create review', 'Exception': str(e)}), 500

# List a single review 
@app.route('/reviews' + '/<int:review_id>', methods=['GET'])
def get_review(review_id):
    with db.connect() as conn:
        # Fetch the review and the associated business details
        stmnt = sqlalchemy.text(
            '''
            SELECT r.review_id, r.user_id, r.business_id, r.stars, r.review_text, b.name as business_name
            FROM reviews r
            JOIN businesses b ON r.business_id = b.business_id
            WHERE r.review_id = :review_id
            '''
        )
        # CHeck if the review exists
        result = conn.execute(stmnt, {'review_id': review_id}).mappings().one_or_none()
        if result is None:
            return jsonify(ERORR_NOT_FOUND_REVIEW), 404
        
        # Construct response
        review = dict(result)
        review['self'] = f"{request.url_root}reviews/{review['review_id']}"
        review['business'] = f"{request.url_root}businesses/{review['business_id']}"
        # Edit the response to remove review_text if it is empty
        if review['review_text'] == '':
            updated_review = {
                'id': review['review_id'],
                'user_id': review['user_id'],
                'business': review['business'],
                'stars': review['stars'],
                'self': review['self']
            }
        else:
            updated_review = {
                'id': review['review_id'],
                'user_id': review['user_id'],
                'business': review['business'],
                'stars': review['stars'],
                'review_text': review['review_text'],
                'self': review['self']}

        return jsonify(updated_review), 200

# Edit a review
@app.route('/reviews' + '/<int:review_id>', methods=['PUT'])
def edit_review(review_id):
    content = request.get_json()
    # Check if all required fields are present
    if 'stars' not in content:
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400

    with db.connect() as conn:
        # Using mappings() to access columns by name
        existing_review = conn.execute(
            sqlalchemy.text("SELECT * FROM reviews WHERE review_id = :review_id"),
            {'review_id': review_id}
        ).mappings().one_or_none()  # Ensures that result can be accessed by column name
        
        if not existing_review:
            return jsonify({"Error": "No review with this review_id exists"}), 404

        # Update the review
        update_fields = {
            'review_id': review_id,
            'stars': content['stars'],
            'review_text': content.get('review_text', existing_review['review_text'])  # Default to existing if not provided
        }
        update_query = "UPDATE reviews SET stars = :stars, review_text = :review_text WHERE review_id = :review_id"
        
        conn.execute(sqlalchemy.text(update_query), update_fields)
        conn.commit()

        # Construct the response
        review_url = f"{request.url_root}reviews/{review_id}"
        business_url = f"{request.url_root}businesses/{existing_review['business_id']}"

        updated_review = {
            'id': review_id,
            'user_id': existing_review['user_id'],
            'business': business_url,
            'stars': content['stars'],
            'review_text': update_fields['review_text'],
            'self': review_url
        }
        
        return jsonify(updated_review), 200

# Delete a review
@app.route('/reviews' + '/<int:review_id>', methods=['DELETE'])
def delete_review(review_id):
    with db.connect() as conn:
        # First, check if the review exists
        existing_review = conn.execute(
            sqlalchemy.text("SELECT 1 FROM reviews WHERE review_id = :review_id"),
            {'review_id': review_id}
        ).scalar()
        
        if not existing_review:
            return jsonify({"Error": "No review with this review_id exists"}), 404

        # Delete the review
        conn.execute(
            sqlalchemy.text("DELETE FROM reviews WHERE review_id = :review_id"),
            {'review_id': review_id}
        )
        conn.commit()

        # Return success status
        return ('', 204)

# List all reviews for a user_id
@app.route('/users/<int:user_id>/reviews', methods=['GET'])
def get_users_reviews(user_id):
    with db.connect() as conn:
        # Fetch all reviews for the specified user
        query = sqlalchemy.text(
            '''
            SELECT r.review_id, r.user_id, r.business_id, r.stars, r.review_text, b.name as business_name
            FROM reviews r
            JOIN businesses b ON r.business_id = b.business_id
            WHERE r.user_id = :user_id
            '''
        )
        # Check if the user has any reviews
        results = conn.execute(query, {'user_id': user_id}).mappings().all()
        if not results:
            return jsonify({"Error": "No reviews found for this user"}), 404

        #Return the reviews
        reviews = []
        for review in results:
            review_dict = {
                'id': review['review_id'],
                'user_id': review['user_id'],
                'business': f"{request.url_root}businesses/{review['business_id']}",
                'stars': review['stars'],
                'review_text': review['review_text'],
                'self': f"{request.url_root}reviews/{review['review_id']}"
            }
            reviews.append(review_dict)

        return jsonify(reviews), 200

##########################
if __name__ == '__main__':
    init_db()
    create_table(db)
    create_reviews_table(db)
    app.run(host='0.0.0.0', port=8000, debug=True)