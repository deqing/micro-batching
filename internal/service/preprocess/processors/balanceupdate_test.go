package processors

import (
	. "github.com/deqing/batching-api/api"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBalanceUpdate(t *testing.T) {
	t.Run(
		"Only Need To Keep Latest Balance Update", func(t *testing.T) {
			b := &BalanceUpdate{}

			email := "email"
			user1To50 := Job_Params{}
			user1To80 := Job_Params{}
			user1To100 := Job_Params{}
			user2To70 := Job_Params{}
			userInfoUpdate := Job_Params{}

			err := user1To50.FromBalanceUpdateParams(BalanceUpdateParams{UserId: "1", Amount: 50})
			err = user1To80.FromBalanceUpdateParams(BalanceUpdateParams{UserId: "1", Amount: 80})
			err = user1To100.FromBalanceUpdateParams(BalanceUpdateParams{UserId: "1", Amount: 100})
			err = user2To70.FromBalanceUpdateParams(BalanceUpdateParams{UserId: "2", Amount: 70})
			err = userInfoUpdate.FromUpdateUserInfoParams(UpdateUserInfoParams{UserId: "2", Email: &email})
			assert.NoError(t, err)

			jobs := b.Process([]Job{
				{Type: BALANCEUPDATE, Params: user1To50},
				{Type: BALANCEUPDATE, Params: user2To70},
				{Type: UPDATEUSERINFO, Params: userInfoUpdate},
				{Type: BALANCEUPDATE, Params: user1To80},
				{Type: BALANCEUPDATE, Params: user1To50},
				{Type: BALANCEUPDATE, Params: user1To100},
			})

			assert.Equal(t, []Job{
				{Type: BALANCEUPDATE, Params: user1To100},
				{Type: BALANCEUPDATE, Params: user2To70},
				{Type: UPDATEUSERINFO, Params: userInfoUpdate},
			}, jobs)
		})
}
