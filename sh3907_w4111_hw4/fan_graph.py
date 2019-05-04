from py2neo import data, Graph, NodeMatcher, Node, Relationship, RelationshipMatcher

import json
import uuid


class FanGraph(object):
    """
    This object provides a set of helper methods for creating and retrieving nodes and relationships from
    a Neo4j database holding information about players, teams, fans, comments and their relationships.
    """

    # Note:
    # I tend to avoid object mapping frameworks. Object mapping frameworks are fun in the beginning
    # but tend to be annoying after a while. So, I did not create types Player, Team, etc.
    #

    # Connects to the DB and sets a Graph instance variable.
    # Also creates a NodeMatcher and RelationshipMatcher, which are a py2neo framework classes.
    def __init__(self, auth=('neo4j', 'sh3907'), host='localhost', port=7687, secure=False, ):
        self._graph = Graph(secure=secure,
                            bolt=True,
                            auth=auth,
                            host=host,
                            port=port)
        self._node_matcher = NodeMatcher(self._graph)
        self._relationship_matcher = RelationshipMatcher(self._graph)

    def run_q(self, qs, args):
        """

        :param qs: Query string that may have {} slots for parameters.
        :param args: Dictionary of parameters to insert into query string.
        :return:  Result of the query, which executes as a single, standalone transaction.
        """
        try:
            tx = self._graph.begin(autocommit=False)
            result = self._graph.run(qs, args)
            return result
        except Exception as e:
            print("Run exaception = ", e)

    def run_match(self, labels=None, properties=None):
        """
        Uses a NodeMatcher to find a node matching a "template."
        :param labels: A list of labels that the node must have.
        :param properties: A dictionary of {property_name: property_value} defining the template that the
            node must match.
        :return: An array of Node objects matching the pattern.
        """
        # ut.debug_message("Labels = ", labels)
        # ut.debug_message("Properties = ", json.dumps(properties))

        if labels is not None and properties is not None:
            result = self._node_matcher.match(labels, **properties)
        elif labels is not None and properties is None:
            result = self._node_matcher.match(labels)
        elif labels is None and properties is not None:
            result = self._node_matcher.match(**properties)
        else:
            raise ValueError("Invalid request. Labels and properties cannot both be None.")

        # Convert NodeMatch data into a simple list of Nodes.
        full_result = []
        for r in result:
            full_result.append(r)

        return full_result

    def find_nodes_by_template(self, tmp):
        """

        :param tmp: A template defining the label and properties for Nodes to return. An
         example is { "label": "Fan", "template" { "last_name": "Ferguson", "first_name": "Donald" }}
        :return: A list of Nodes matching the template.
        """
        labels = tmp.get('label', None)
        props = tmp.get("template", None)
        result = self.run_match(labels=labels, properties=props)
        return result

    # Create and save a new node for  a 'Fan.'
    def create_fan(self, uni, last_name, first_name):
        """

        :param uni: uni
        :param last_name: Obvious
        :param first_name: Obvious
        :return: Node created.

        NOTE: This does not check uni uniqueness. We could do using transactions or setting a constraint
        on the database.
        """
        n = Node("Fan", uni=uni, last_name=last_name, first_name=first_name)
        tx = self._graph.begin(autocommit=True)
        tx.create(n)
        return n

    # Given a UNI, return the node for the Fan.
    def get_fan(self, uni):
        n = self.find_nodes_by_template({"label": "Fan", "template": {"uni": uni}})
        if n is not None and len(n) > 0:
            # I should throw an exception here if there is more than 1.
            n = n[0]
        else:
            n = None

        return n

    def create_player(self, player_id, last_name, first_name):
        n = Node("Player", player_id=player_id, last_name=last_name, first_name=first_name)
        tx = self._graph.begin(autocommit=True)
        tx.create(n)
        return n

    def get_player(self, player_id):
        n = self.find_nodes_by_template({"label": "Player", "template": {"player_id": player_id}})
        if n is not None and len(n) > 0:
            n = n[0]
        else:
            n = None

        return n

    def create_team(self, team_id, team_name):
        n = Node("Team", team_id=team_id, team_name=team_name)
        tx = self._graph.begin(autocommit=True)
        tx.create(n)
        return n

    def get_team(self, team_id):
        n = self.find_nodes_by_template({"label": "Team", "template": {"team_id": team_id}})
        if n is not None and len(n) > 0:
            n = n[0]
        else:
            n = None

        return n

    def create_supports(self, uni, team_id):
        """
        Create a SUPPORTS relationship from a Fan to a Team.
        :param uni: The UNI for a fan.
        :param team_id: An ID for a team.
        :return: The created SUPPORTS relationship from the Fan to the Team
        """
        f = self.get_fan(uni)
        t = self.get_team(team_id)
        r = Relationship(f, "SUPPORTS", t)
        tx = self._graph.begin(autocommit=True)
        tx.create(r)
        return r

    def get_appearance(self, player_id, team_id, year_id):
        """
        Get the information about appearances for a player and team.
        :param player_id: player_id
        :param team_id: team_id
        :param year_id: The year for getting appearances.
        :return:
        """
        try:
            # Get the Nodes at the ends of the relationship representing appearances.
            p = self.get_player(player_id)
            t = self.get_team(team_id)

            # Run a match looking for relationships of a specific type linking the nodes.
            rm = self._graph.match(nodes=[p, t], r_type="APPEARED")
            result = []

            # If there is a list of relationships.
            if rm is not None:
                for r in rm:

                    # The type will be a class APPEARED() because of the OO mapping.
                    node_type = type(r).__name__
                    year = r['year']

                    # If the type and year are correct, add to result
                    if node_type == "APPEARED" and (year == year_id or year_id is None):
                        result.append(r)

                return result
            else:
                return None
        except Exception as e:
            print("get_appearance: Exception e = ", e)
            raise e

    # Create an APPEARED relationship from a player to a Team
    def create_appearance_all(self, player_id, team_id, year, games):
        """

        :param player_id: O
        :param team_id:
        :param year:
        :param games:
        :return:
        """
        try:
            tx = self._graph.begin(autocommit=False)
            q = "match (n:Player {player_id: '" + player_id + "'}), " + \
                "(t:Team {team_id: '" + team_id + "'}) " + \
                "create (n)-[r:APPEARED { games: " + str(games) + ", year : " + str(year) + \
                "}]->(t)"
            result = self._graph.run(q)
            tx.commit()
        except Exception as e:
            print("create_appearances: exception = ", e)

    # Create a FOLLOWS relationship from a Fan to another Fan.
    def create_follows(self, follower, followed):
        f = self.get_fan(follower)
        t = self.get_fan(followed)
        r = Relationship(f, "FOLLOWS", t)
        tx = self._graph.begin(autocommit=True)
        tx.create(r)

    # -------- Sin-Yi Huang's implementation starts from here
    def get_comment(self, comment_id):
        """

        :param comment_id: Comment ID
        :return: Comment
        """
        # create a matcher
        matcher = NodeMatcher(self._graph)

        # connect fan node to comment node
        comment_n = matcher.match("Comment", comment_id=comment_id).first()

        # return type: Node
        return comment_n

    def create_comment(self, uni, comment, team_id=None, player_id=None):
        """
        Creates a comment
        :param uni: The UNI for the Fan making the comment.
        :param comment: A simple string.
        :param team_id: A valid team ID or None. team_id and player_id cannot BOTH be None.
        :param player_id: A valid player ID or None
        :return: The Node representing the comment.
        """
        try:
            tx = self._graph.begin(autocommit=False)
            # create a random id for comment
            cid = uuid.uuid4()

            # create comment node
            comment_n = Node("Comment", comment=comment, comment_id=str(cid))
            tx.create(comment_n)
            print(comment_n)

            # create a matcher
            matcher = NodeMatcher(self._graph)

            # connect fan node to comment node
            fan_n = matcher.match("Fan", uni=uni).first()
            rela = Relationship(fan_n, "COMMENT_BY", comment_n)
            tx.create(rela)

            # connect comment node to team node
            if team_id:
                team_n = matcher.match("Team", team_id=team_id).first()
                rela = Relationship(comment_n, "COMMENT_ON", team_n)
                tx.create(rela)

            # connect comment node to player node
            if player_id:
                player_n = matcher.match("Player", player_id=player_id).first()
                rela = Relationship(comment_n, "COMMENT_ON", player_n)
                tx.create(rela)

            tx.commit()

        except Exception as e:
            print("create_comment: exception = ", e)

        # return type: Node
        return comment_n

    def create_sub_comment(self, uni, origin_comment_id, comment):
        """
        Create a sub-comment (response to a comment or response) and links with parent in thread.
        :param uni: ID of the Fan making the comment.
        :param origin_comment_id: Id of the comment to which this is a response.
        :param comment: Comment string
        :return: Created comment.
        """
        try:
            tx = self._graph.begin(autocommit=False)

            # create a random id for comment
            cid = uuid.uuid4()

            # create comment node
            comment_n = Node("Comment", comment=comment, comment_id=str(cid))
            tx.create(comment_n)

            # create a matcher
            matcher = NodeMatcher(self._graph)

            # connect fan node to comment node
            fan_n = matcher.match("Fan", uni=uni).first()
            rela = Relationship(fan_n, "COMMENT_BY", comment_n)
            tx.create(rela)

            # connect comment node to original comment node
            origin_comment_n = matcher.match("Comment", comment_id=origin_comment_id).first()
            rela = Relationship(comment_n, "COMMENT_ON", origin_comment_n)
            tx.create(rela)

            tx.commit()

        except Exception as e:
            print("create_comment: exception = ", e)

        # return type: Node
        return comment_n

    def get_sub_comments(self, comment_id):
        """

        :param comment_id: The unique ID of the comment.
        :return: The sub-comments.
        """
        # create a matcher
        matcher = NodeMatcher(self._graph)

        comment_n = matcher.match("Comment", comment_id=comment_id).first()

        sub_comments = RelationshipMatcher(self._graph).match((None, comment_n), "COMMENT_ON")

        # return type: RelationshipMatch
        return sub_comments

    def get_player_comments(self, player_id):
        """
        Gets all of the comments associated with a player, Also returns the Nodes for people making the comments.
        :param player_id: ID of the player.
        :return: Graph containing comment, comment streams and commenters.
        """
        tx = self._graph.begin(autocommit=False)

        q = "MATCH (n:Player {player_id:'" + player_id + "'}) MATCH (c:Comment)-[r:COMMENT_ON]->(n) RETURN c;"

        result = self._graph.run(q)

        tx.commit()

        # return type: Cursor
        return result

    def get_team_comments(self, team_id):
        """
        Gets all of the comments associated with a team.  Also returns the Nodes for people making the comments.
        :param player_id: ID of the team.
        :return: Graph containing comment, comment streams and commenters.
        """
        # create a matcher
        matcher = NodeMatcher(self._graph)

        team_n = matcher.match("Team", team_id=team_id).first()

        comments = RelationshipMatcher(self._graph).match((None, team_n), "COMMENT_ON")

        # return type: RelationshipMatch
        return comments

    def get_players_by_team(self, team_id, yearid):
        """

        :param team_id: The ID of a team.
        :param yearid: A year.
        :return: Returns the players who played for the team in the year.
        """
        tx = self._graph.begin(autocommit=False)

        q = "MATCH (t:Team {team_id:'" + team_id + "'}) MATCH (p:Player)-[r:APPEARED]->(n) WHERE r.year = " + yearid + " RETURN p;"

        result = self._graph.run(q)

        tx.commit()

        # return type: Cursor
        return result
