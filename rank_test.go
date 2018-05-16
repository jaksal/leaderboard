package rank

import (
	"fmt"
	"log"
	"strconv"
	"testing"
)

var lbName = "test_lb"

func init() {
	if err := InitRedis("10.10.5.33:50005"); err != nil {
		log.Fatal(err)
	}
	DeleteLeaderboard(lbName)
}

func rankMembersInLeaderboard(membersToAdd int) error {
	if membersToAdd == 0 {
		membersToAdd = 5
	}

	for i := 1; i < membersToAdd+1; i++ {
		member := "member_" + strconv.Itoa(i)
		if err := RankMember(lbName, member, i); err != nil {
			return fmt.Errorf("leaderboard.RankMember %d err=%s", i, err)
		}
	}

	return nil
}

func TestMembers(t *testing.T) {
	///////////////////////////////////////////////////////////////////////////
	defer DeleteLeaderboard(lbName)

	if err := RankMember(lbName, "member_1", 50); err != nil {
		t.Error("RankMember err", err)
	}
	RankMember(lbName, "member_2", 50)
	RankMember(lbName, "member_3", 30)
	RankMember(lbName, "member_4", 30)
	RankMember(lbName, "member_5", 10)

	members, _ := Members(lbName, 1, 0)
	if len(members) != 5 {
		t.Error("Members Err", members)
	} else {
		if members[0].rank != 1 {
			t.Error("Leaderboard Leaders Err!", members[0].rank)
		}
		if members[1].rank != 1 {
			t.Error("Leaderboard Leaders Err!", members[1].rank)
		}
		if members[2].rank != 3 {
			t.Error("Leaderboard Leaders Err!", members[2].rank)
		}
		if members[3].rank != 3 {
			t.Error("Leaderboard Leaders Err!", members[3].rank)
		}
		if members[4].rank != 5 {
			t.Error("Leaderboard Leaders Err!", members[4].rank)
		}
	}

}

func TestAroundMe(t *testing.T) {
	///////////////////////////////////////////////////////////////////////////
	defer DeleteLeaderboard(lbName)

	RankMember(lbName, "member_1", 50)
	RankMember(lbName, "member_2", 50)
	RankMember(lbName, "member_3", 30)
	RankMember(lbName, "member_4", 30)
	RankMember(lbName, "member_5", 10)
	RankMember(lbName, "member_6", 50)
	RankMember(lbName, "member_7", 50)
	RankMember(lbName, "member_8", 30)
	RankMember(lbName, "member_9", 30)
	RankMember(lbName, "member_10", 10)

	members, _ := Members(lbName, 1, 3)

	if members[0].rank != 1 ||
		members[1].rank != 1 ||
		members[2].rank != 1 {
		t.Error("Leaderboard Members Err!", members)
	}

	members, _ = Members(lbName, 2, 3)
	if members[0].rank != 1 ||
		members[1].rank != 5 ||
		members[2].rank != 5 {
		t.Error("Leaderboard Members Err!", members)
	}

	members, _ = AroundMe(lbName, "member_4", 0)
	if members[0].rank != 1 ||
		members[4].rank != 5 ||
		members[9].rank != 9 {
		t.Error("Leaderboard AroundMe Err!", members)
	}
}

func TestRankFor(t *testing.T) {
	///////////////////////////////////////////////////////////////////////////
	defer DeleteLeaderboard(lbName)

	RankMember(lbName, "member_1", 50)
	RankMember(lbName, "member_2", 50)
	RankMember(lbName, "member_3", 30)

	if rank, _ := RankFor(lbName, "member_1"); rank != 1 {
		t.Error("Leaderboard RankFor Err!", rank)
	}
	if rank, _ := RankFor(lbName, "member_2"); rank != 1 {
		t.Error("Leaderboard RankFor Err!", rank)
	}
	if rank, _ := RankFor(lbName, "member_3"); rank != 3 {
		t.Error("Leaderboard RankFor Err!", rank)
	}

	if member, _ := ScoreAndRankFor(lbName, "member_1"); member.rank != 1 {
		t.Error("Leaderboard ScoreAndRankFor Err!", member)
	}
	if member, _ := ScoreAndRankFor(lbName, "member_2"); member.rank != 1 {
		t.Error("Leaderboard ScoreAndRankFor Err!", member)
	}
	if member, _ := ScoreAndRankFor(lbName, "member_3"); member.rank != 3 {
		t.Error("Leaderboard ScoreAndRankFor Err!", member)
	}

}

func TestScoreFor(t *testing.T) {
	///////////////////////////////////////////////////////////////////////////
	defer DeleteLeaderboard(lbName)

	RankMember(lbName, "member_1", 50)
	RankMember(lbName, "member_2", 40)
	RankMember(lbName, "member_3", 30)
	RankMember(lbName, "member_4", 20)
	RankMember(lbName, "member_5", 10)
	ChangeScoreFor(lbName, "member_3", 1)

	if rank, _ := RankFor(lbName, "member_3"); rank != 3 {
		t.Error("Leaderboard RankFor Err!", rank)
	}
	if rank, _ := RankFor(lbName, "member_4"); rank != 4 {
		t.Error("Leaderboard RankFor Err!", rank)
	}
	if score, _ := ScoreFor(lbName, "member_3"); score != 31 {
		t.Error("Leaderboard ScoreFor Err!", score)
	}
}

func TestRemoveMembersInScoreRange(t *testing.T) {
	///////////////////////////////////////////////////////////////////////////
	defer DeleteLeaderboard(lbName)

	cnt := 5
	rankMembersInLeaderboard(cnt)

	if totalMembers, _ := TotalMembers(lbName); totalMembers != cnt {
		t.Error("NewLeaderBoard TotalMembers Err! ", totalMembers)
	}

	RankMember(lbName, "cheater_1", 100)
	RankMember(lbName, "cheater_2", 101)
	RankMember(lbName, "cheater_3", 102)

	if totalMembers, _ := TotalMembers(lbName); totalMembers != cnt+3 {
		t.Error("NewLeaderBoard TotalMembers Err! ", totalMembers)
	}

	RemoveMembersInScoreRange(lbName, 100, 102)
	if totalMembers, _ := TotalMembers(lbName); totalMembers != cnt {
		t.Error("NewLeaderBoard TotalMembers Err! ", totalMembers)
	}

	members, _ := Members(lbName, 1, 0)
	for _, member := range members {
		if member.score > 100 {
			t.Error("NewLeaderBoard RemoveMembersInScoreRange Err! ", member)
		}
	}

}

func TestTotalMembers(t *testing.T) {
	///////////////////////////////////////////////////////////////////////////
	defer DeleteLeaderboard(lbName)

	cnt := DEFAULT_PAGESIZE
	rankMembersInLeaderboard(cnt)

	if totalMembers, _ := TotalMembers(lbName); totalMembers != cnt {
		t.Error("NewLeaderBoard TotalMembers Err! ", totalMembers)
	}

	if members, _ := MembersFromRankRange(lbName, 5, 9); len(members) != 5 ||
		members[0].Member != "member_21" || members[0].score != 21 ||
		members[4].Member != "member_17" {
		t.Error("NewLeaderBoard MembersFromRankRange Err! ", members)
	}

	if members, _ := MembersFromRankRange(lbName, 1, 1); len(members) != 1 ||
		members[0].Member != "member_25" || members[0].score != 25 {
		t.Error("NewLeaderBoard MembersFromRankRange Err! ", members)
	}

	if members, _ := MembersFromRankRange(lbName, -1, 26); len(members) != 25 ||
		members[0].Member != "member_25" || members[0].score != 25 ||
		members[24].Member != "member_1" {
		t.Error("NewLeaderBoard MembersFromRankRange Err! ", members)
	}

	if members, _ := Top(lbName, 5); len(members) != 5 ||
		members[0].Member != "member_25" || members[0].score != 25 ||
		members[4].Member != "member_21" {
		t.Error("NewLeaderBoard Top Err! ", members)
	}

	if members, _ := Top(lbName, 1); len(members) != 1 ||
		members[0].Member != "member_25" || members[0].score != 25 {
		t.Error("NewLeaderBoard Top Err! ", members)
	}

	if members, _ := Top(lbName, 26); len(members) != 25 ||
		members[0].Member != "member_25" || members[0].score != 25 ||
		members[24].Member != "member_1" {
		t.Error("NewLeaderBoard Top Err! ", members)
	}

	rankedMembers := RankedInList(lbName, []string{"member_1", "member_5", "member_10"})
	if len(rankedMembers) != 3 {
		t.Error("NewLeaderBoard RankedInList Err! ", len(rankedMembers))
	}

	if rankedMembers[0].rank != 25 || rankedMembers[0].score != 1 {
		t.Error("NewLeaderBoard RankedInList Err! ", rankedMembers[0])
	}

	if rankedMembers[1].rank != 21 || rankedMembers[1].score != 5 {
		t.Error("NewLeaderBoard RankedInList Err! ", rankedMembers[1])
	}

	if rankedMembers[2].rank != 16 || rankedMembers[2].score != 10 {
		t.Error("NewLeaderBoard RankedInList Err! ", rankedMembers[2])
	}
	/*
		rankedMembers = RankedInList(lbName, []string{"member_5", "member_1", "member_10"}, &REQUEST_OPTIONS{sortBy: "rank"})
		if len(rankedMembers) != 3 {
			t.Error("NewLeaderBoard RankedInList Err! ", len(rankedMembers))
		}

		if rankedMembers[0].rank != 16 || rankedMembers[0].score != 10 {
			t.Error("NewLeaderBoard RankedInList Err! ", rankedMembers[0])
		}

		if rankedMembers[1].rank != 21 || rankedMembers[1].score != 5 {
			t.Error("NewLeaderBoard RankedInList Err! ", rankedMembers[1])
		}

		if rankedMembers[2].rank != 25 || rankedMembers[2].score != 1 {
			t.Error("NewLeaderBoard RankedInList Err! ", rankedMembers[2])
		}

		rankedMembers = RankedInList([]string{"member_5", "member_1", "member_10"}, &REQUEST_OPTIONS{sortBy: "score"})
		if len(rankedMembers) != 3 {
			t.Error("NewLeaderBoard RankedInList Err! ", len(rankedMembers))
		}

		if rankedMembers[0].rank != 25 || rankedMembers[0].score != 1 {
			t.Error("NewLeaderBoard RankedInList Err! ", rankedMembers[0])
		}

		if rankedMembers[1].rank != 21 || rankedMembers[1].score != 5 {
			t.Error("NewLeaderBoard RankedInList Err! ", rankedMembers[1])
		}

		if rankedMembers[2].rank != 16 || rankedMembers[2].score != 10 {
			t.Error("NewLeaderBoard RankedInList Err! ", rankedMembers[2])
		}
	*/

	rankedMembers = RankedInList(lbName, []string{"member_1", "member_5", "jones"})
	if len(rankedMembers) != 3 {
		t.Error("NewLeaderBoard RankedInList Err! ", len(rankedMembers))
	}
	if rankedMembers[2].rank != -1 {
		t.Error("NewLeaderBoard RankedInList Err! ", rankedMembers[2])
	}

	/*
		rankedMembers = RankedInList([]string{"member_1", "member_5", "jones"}, &REQUEST_OPTIONS{includeMissing: true})
		if len(rankedMembers) != 2 {
			t.Error("NewLeaderBoard RankedInList Err! ", len(rankedMembers))
		}
		if rankedMembers[0].Member != "member_1" ||
			rankedMembers[1].Member != "member_5" {
			t.Error("NewLeaderBoard RankedInList Err! ", rankedMembers[1])
		}
	*/
}

func TestRankMemberEx(t *testing.T) {
	defer DeleteLeaderboard(lbName)

	if rank, err := RankMemberEx(lbName, "aaa", 10); err != nil {
		t.Error("RankMemberEx error", err)
	} else {
		if rank != 1 {
			t.Error("RankMemberEx result err", rank)
		}
	}

	if rank, err := RankMemberEx(lbName, "bbb", 20); err != nil {
		t.Error("RankMemberEx error", err)
	} else {
		if rank != 1 {
			t.Error("RankMemberEx result err", rank)
		}
	}

	if rank, err := RankMemberEx(lbName, "aaa", 15); err != nil {
		t.Error("RankMemberEx error", err)
	} else {
		if rank != 2 {
			t.Error("RankMemberEx result err", rank)
		}
	}
}
