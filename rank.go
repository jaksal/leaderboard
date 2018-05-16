package rank

import (
	"math"
	"strconv"

	"fmt"

	"github.com/gomodule/redigo/redis"
)

var conn redis.Conn

// InitRedis init redis connection
func InitRedis(conf string) error {
	// init redigo.
	c, err := redis.Dial("tcp", conf)
	if err != nil {
		return err
	}
	conn = c
	return nil
}

// Final close redis connection
func Final() {
	if conn != nil {
		conn.Close()
	}
}

// DEFAULT_PAGESIZE : 25
const DEFAULT_PAGESIZE int = 25

// RankScore : member score struct.
type RankScore struct {
	Member string
	score  int
	rank   int
}

// GetScore get rank score
func (m *RankScore) GetScore() int {
	return m.score
}

// GetRank get rank
func (m *RankScore) GetRank() int {
	return m.rank
}

func (m *RankScore) String() string {
	return fmt.Sprintf("member:%s score:%d rank:%d", m.Member, m.score, m.rank)
}

// RankScores : rank score list
type RankScores []*RankScore

/*
func (p RankScores) Len() int           { return len(p) }
func (p RankScores) Less(i, j int) bool { return p[i].rank > p[j].rank }
func (p RankScores) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
*/

// RankMember :   Rank a member in the leaderboard.
func RankMember(lbName string, member string, score int) error {
	_, err := conn.Do("ZADD", lbName, score, member)
	return err
}

// RankMembers : Rank an array of members in the leaderboard.
func RankMembers(lbName string, membersAndScores []*RankScore) error {
	for _, memberScore := range membersAndScores {
		conn.Send("ZADD", lbName, memberScore.score, memberScore.Member)
	}
	conn.Flush()
	return nil
}

// RemoveMember : Remove a member from the leaderboard.
func RemoveMember(lbName string, member string) error {
	_, err := conn.Do("ZREM", lbName, member)
	return err
}

// TotalMembers : Retrieve the total number of members in the leaderboard.
func TotalMembers(lbName string) (int, error) {
	count, err := redis.Int(conn.Do("ZCARD", lbName))
	if err != nil {
		return -1, err
	}
	return count, nil
}

// TotalPages : Retrieve the total number of pages in the leaderboard.
func TotalPages(lbName string, pageSize int) int {
	if pageSize < 1 {
		pageSize = DEFAULT_PAGESIZE
	}

	totalMembers, _ := TotalMembers(lbName)
	return int(math.Ceil(float64(totalMembers) / float64(pageSize)))
}

// TotalMembersInScoreRange : Retrieve the total members in a given score range from the leaderboard.
func TotalMembersInScoreRange(lbName string, minScore int, maxScore int) (int, error) {
	count, err := redis.Int(conn.Do("ZCOUNT", lbName, minScore, maxScore))
	if err != nil {
		return -1, err
	}
	return count, nil
}

// ChangeScoreFor : Change the score for a member in the leaderboard by a score delta which can be positive or negative.
func ChangeScoreFor(lbName string, member string, delta int) error {
	_, err := conn.Do("ZINCRBY", lbName, delta, member)
	return err
}

// CheckMember : Check to see if a member exists in the leaderboard.
func CheckMember(lbName string, member string) (bool, error) {
	res, err := conn.Do("ZSCORE", lbName, member)
	if err != nil {
		return false, err
	}
	if res == nil {
		return false, nil
	}
	return true, nil
}

// RankMemberEx :   Rank a member in the leaderboard.
func RankMemberEx(lbName string, member string, score int) (int, error) {
	// get current score
	res, err := conn.Do("ZSCORE", lbName, member)
	if err != nil {
		return 0, err
	}
	if res == nil {
		// not found exist score . add new score.
		_, err := conn.Do("ZADD", lbName, score, member)
		if err != nil {
			return 0, err
		}
	} else {
		existScore, _ := redis.Int(res, nil)
		// compare new score.
		if existScore != score {
			newScore, err := redis.Int(conn.Do("ZINCRBY", lbName, score-existScore, member))
			if err != nil {
				return 0, err
			}
			if newScore != score {
				return 0, fmt.Errorf("unexpected score %d,%d", newScore, score)
			}
		}
	}

	// get new rank..
	param1 := "(" + strconv.Itoa(score)
	param2 := "+inf"

	rank, err := redis.Int(conn.Do("ZCOUNT", lbName, param1, param2))
	if err != nil {
		return 0, err
	}

	return rank + 1, nil
}

// ScoreFor : Retrieve the score for a member in the leaderboard.
func ScoreFor(lbName string, member string) (int, error) {
	score, err := redis.Int(conn.Do("ZSCORE", lbName, member))
	if err != nil {
		return -1, err
	}
	return score, nil
}

// RankFor : Retrieve the rank for a member in the leaderboard.
func RankFor(lbName string, member string) (int, error) {
	score, err := redis.Int(conn.Do("ZSCORE", lbName, member))
	if err != nil {
		return -1, err
	}

	param1 := "(" + strconv.Itoa(score)
	param2 := "+inf"

	rank, err := redis.Int(conn.Do("ZCOUNT", lbName, param1, param2))
	if err != nil {
		return -1, err
	}

	return rank + 1, nil
}

// ScoreAndRankFor : Retrieve the score and rank for a member in the leaderboard.
func ScoreAndRankFor(lbName string, member string) (*RankScore, error) {
	score, err := redis.Int(conn.Do("ZSCORE", lbName, member))
	if err != nil {
		return nil, err
	}

	param1 := "(" + strconv.Itoa(score)
	param2 := "+inf"

	rank, err := redis.Int(conn.Do("ZCOUNT", lbName, param1, param2))
	if err != nil {
		return nil, err
	}

	return &RankScore{Member: member, score: score, rank: rank + 1}, nil
}

// RemoveMembersInScoreRange : Remove members from the leaderboard in a given score range.
func RemoveMembersInScoreRange(lbName string, minScore int, maxScore int) error {
	_, err := conn.Do("ZREMRANGEBYSCORE", lbName, minScore, maxScore)
	return err
}

// RemoveMembersOutsideRank : Remove members from the leaderboard outside a given rank.
func RemoveMembersOutsideRank(lbName string, rank int) (int, error) {
	rankStart := 0
	rankEnd := -(rank) - 1

	count, err := redis.Int(conn.Do("ZREMRANGEBYRANK", lbName, rankStart, rankEnd))
	if err != nil {
		return -1, err
	}
	return count, nil
}

// PercentileFor : Retrieve the percentile for a member in the leaderboard.
// @param member [String] Member name.
// @return the percentile for a member in the leaderboard. Return +nil+ for a non-existent member.
func PercentileFor(lbName string, member string) (int, error) {
	if ok, err := CheckMember(lbName, member); err != nil || ok == false {
		return -1, err
	}

	count, err := redis.Int(conn.Do("ZCARD", lbName))
	if err != nil {
		return -1, err
	}

	rank, err := redis.Int(conn.Do("ZREVRANK", lbName, member))
	if err != nil {
		return -1, err
	}

	return int(math.Ceil(float64(count-rank-1) / float64(count) * 100.0)), nil
}

// ScoreForPercentile : Calculate the score for a given percentile value in the leaderboard.
func ScoreForPercentile(lbName string, percentile int) (int, error) {
	if percentile < 0 || percentile > 100 {
		return -1, nil
	}

	totalMembers, err := TotalMembers(lbName)
	if err != nil || totalMembers < 1 {
		return -1, err
	}

	index := float64((float64(totalMembers) - 1.0) * (float64(percentile) / 100.0))

	values, err := redis.Strings(conn.Do("ZREVRANGE", lbName, math.Floor(index), math.Ceil(index), "WITHSCORES"))
	if err != nil {
		return -1, err
	}
	// Response format: ["Alice", "123", "Bob", "456"] (i.e. flat list, not member/score tuples)
	lowScore, _ := strconv.Atoi(values[1])

	if index == math.Floor(index) {
		return lowScore, nil
	}

	interpolateFraction := int(index - math.Floor(index))
	hiScore, _ := strconv.Atoi(values[3])

	return lowScore + interpolateFraction*(hiScore-lowScore), nil
}

// PageFor : Determine the page where a member falls in the leaderboard.
func PageFor(lbName string, member string, pageSize int) (int, error) {
	if pageSize < 1 {
		pageSize = DEFAULT_PAGESIZE
	}

	rank, err := RankFor(lbName, member)
	if err != nil {
		return -1, err
	}

	return int(math.Ceil(float64(rank) / float64(pageSize))), nil
}

// RankedInList : Retrieve a page of leaders from the leaderboard for a given list of members.
func RankedInList(lbName string, members []string) []*RankScore {
	var ranksForMembers []*RankScore

	if len(members) == 0 {
		return ranksForMembers
	}

	// Get Score
	for _, member := range members {
		memberScore := &RankScore{Member: member}

		if score, err := redis.Int(conn.Do("ZSCORE", lbName, member)); err == nil {
			memberScore.score = score
		} else {
			memberScore.score = -1
		}
		ranksForMembers = append(ranksForMembers, memberScore)
	}

	// Get Rank.and MemberData
	for i, memberScore := range ranksForMembers {
		if memberScore.score == -1 {
			ranksForMembers[i].rank = -1
			continue
		}

		param1 := "(" + strconv.Itoa(memberScore.score)
		param2 := "+inf"

		if rank, err := redis.Int(conn.Do("ZCOUNT", lbName, param1, param2)); err == nil {
			ranksForMembers[i].rank = rank + 1
		} else {
			ranksForMembers[i].rank = -1
		}
	}

	return ranksForMembers
}

// Members : Retrieve a page of Members from the leaderboard.
func Members(lbName string, currentPage int, pageSize int) ([]*RankScore, error) {
	if currentPage < 1 {
		currentPage = 1
	}
	if pageSize < 1 {
		pageSize = DEFAULT_PAGESIZE
	}

	if totalPage := TotalPages(lbName, pageSize); currentPage > totalPage {
		currentPage = totalPage
	}

	indexForRedis := currentPage - 1

	startingOffset := (indexForRedis * pageSize)
	if startingOffset < 0 {
		startingOffset = 0
	}

	endingOffset := (startingOffset + pageSize) - 1

	members, err := redis.Strings(conn.Do("ZREVRANGE", lbName, startingOffset, endingOffset))
	if err != nil {
		return []*RankScore{}, err
	}
	return RankedInList(lbName, members), nil
}

// AllMembers : Retrieve all Members from the leaderboard.
func AllMembers(lbName string) ([]*RankScore, error) {

	members, err := redis.Strings(conn.Do("ZREVRANGE", lbName, 0, -1))
	if err != nil {
		return []*RankScore{}, err
	}
	return RankedInList(lbName, members), nil
}

// MembersFromScoreRange : Retrieve members from the leaderboard within a given score range.
func MembersFromScoreRange(lbName string, minimumScore int, maximumScore int) ([]*RankScore, error) {

	startScore := minimumScore
	endScore := maximumScore

	members, err := redis.Strings(conn.Do("ZREVRANGEBYSCORE", lbName, startScore, endScore))
	if err != nil {
		return []*RankScore{}, err
	}

	return RankedInList(lbName, members), nil
}

// MembersFromRankRange : Retrieve members from the leaderboard within a given rank range.
func MembersFromRankRange(lbName string, startingRank int, endingRank int) ([]*RankScore, error) {

	startingRank = startingRank - 1
	if startingRank < 0 {
		startingRank = 0
	}
	endingRank = endingRank - 1

	totalMembers, _ := TotalMembers(lbName)
	if endingRank > totalMembers {
		endingRank = totalMembers - 1
	}

	members, err := redis.Strings(conn.Do("ZREVRANGE", lbName, startingRank, endingRank))
	if err != nil {
		return []*RankScore{}, err
	}

	return RankedInList(lbName, members), nil
}

// Top : Retrieve members from the leaderboard within a range from 1 to the number given.
func Top(lbName string, number int) ([]*RankScore, error) {
	return MembersFromRankRange(lbName, 1, number)
}

// MemberAt : Retrieve a member at the specified index from the leaderboard.
func MemberAt(lbName string, position int) (*RankScore, error) {
	members, err := MembersFromRankRange(lbName, position, position)
	if err != nil {
		return nil, err
	}
	if len(members) >= 1 {
		return members[0], nil
	}
	return nil, nil
}

// AroundMe : Retrieve a page of leaders from the leaderboard around a given member.
func AroundMe(lbName string, member string, pageSize int) ([]*RankScore, error) {

	if pageSize < 1 {
		pageSize = DEFAULT_PAGESIZE
	}

	rank, err := redis.Int(conn.Do("ZREVRANK", lbName, member))
	if err != nil {
		return []*RankScore{}, err
	}

	startingOffset := rank - (pageSize / 2)
	if startingOffset < 0 {
		startingOffset = 0
	}
	endingOffset := (startingOffset + pageSize) - 1

	members, err := redis.Strings(conn.Do("ZREVRANGE", lbName, startingOffset, endingOffset))
	if err != nil {
		return []*RankScore{}, err
	}

	return RankedInList(lbName, members), nil
}

// DeleteLeaderboard : Delete the current leaderboard.
func DeleteLeaderboard(lbName string) error {
	_, err := conn.Do("DEL", lbName)
	return err
}
